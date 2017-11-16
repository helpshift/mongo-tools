// Package mongooplog polls operations from the replication oplog of one server, and applies them to another.
package mongooplog

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/util"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"time"
)

// MongoOplog is a container for the user-specified options for running mongooplog.
type MongoOplog struct {
	// standard tool options
	ToolOptions *options.ToolOptions

	// mongooplog-specific options
	SourceOptions *SourceOptions

	// session provider for the source server
	SessionProviderFrom *db.SessionProvider

	// session provider for the destination server
	SessionProviderTo *db.SessionProvider
}

// Dump oplog data for debugging
func printOplog(oplog *db.Oplog) error {
	value, bsonerr := bsonutil.GetBSONValueAsJSON(oplog.Timestamp)
	log.Logvf(log.DebugLow, "oplog : Timestamp %v", value)
	value, bsonerr = bsonutil.GetBSONValueAsJSON(oplog.HistoryID)
	log.Logvf(log.DebugLow, "oplog : HistoryID %v", value)
	value, bsonerr = bsonutil.GetBSONValueAsJSON(oplog.Version)
	log.Logvf(log.DebugLow, "oplog : Version %v", value)
	value, bsonerr = bsonutil.GetBSONValueAsJSON(oplog.Operation)
	log.Logvf(log.DebugLow, "oplog : Operation %v", value)
	value, bsonerr = bsonutil.GetBSONValueAsJSON(oplog.Namespace)
	log.Logvf(log.DebugLow, "oplog : Namespace %v", value)
	value, bsonerr = bsonutil.GetBSONValueAsJSON(oplog.Object)
	log.Logvf(log.DebugLow, "oplog : Object %v", value)
	value, bsonerr = bsonutil.GetBSONValueAsJSON(oplog.Query)
	log.Logvf(log.DebugLow, "oplog : Query %v", value)
	return bsonerr
}

// Run executes the mongooplog program.
func (mo *MongoOplog) Run() error {

	// split up the oplog namespace we are using
	oplogDB, oplogColl, err :=
		util.SplitAndValidateNamespace(mo.SourceOptions.OplogNS)

	if err != nil {
		return err
	}

	// the full oplog namespace needs to be specified
	if oplogColl == "" {
		return fmt.Errorf("the oplog namespace must specify a collection")
	}

	log.Logvf(log.DebugLow, "using oplog namespace `%v.%v`", oplogDB, oplogColl)

	// connect to the destination server
	toSession, err := mo.SessionProviderTo.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to destination db: %v", err)
	}
	defer toSession.Close()
	toSession.SetSocketTimeout(0)

	// purely for logging
	destServerStr := mo.ToolOptions.Host
	if mo.ToolOptions.Port != "" {
		destServerStr = destServerStr + ":" + mo.ToolOptions.Port
	}
	log.Logvf(log.DebugLow, "successfully connected to destination server `%v`", destServerStr)

	// connect to the source server
	fromSession, err := mo.SessionProviderFrom.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to source db: %v", err)
	}
	defer fromSession.Close()
	fromSession.SetSocketTimeout(0)

	log.Logvf(log.DebugLow, "successfully connected to source server `%v`", mo.SourceOptions.From)

	// set slave ok
	fromSession.SetMode(mgo.Eventual, true)

	// get the tailing cursor for the source server's oplog
	tail := buildTailingCursor(fromSession.DB(oplogDB).C(oplogColl),
		mo.SourceOptions)
	defer tail.Close()

	// read the cursor dry, applying ops to the destination
	// server in the process
	oplogEntry := &db.Oplog{}
	res := &db.ApplyOpsResponse{}

	log.Logv(log.DebugLow, "applying oplog entries...")

	opCount := 0

	for tail.Next(oplogEntry) {

		// skip noops
		if oplogEntry.Operation == "n" {
			log.Logvf(log.DebugHigh, "skipping no-op for namespace `%v`", oplogEntry.Namespace)
			continue
		}
		opCount++

		// log every 100th oplog to show progress
		if opCount%100 == 1 {
			log.Logvf(log.DebugLow, "applying oplog %v ", opCount)
			printOplog(oplogEntry)
		}
		// prepare the op to be applied
		opsToApply := []db.Oplog{*oplogEntry}

		// apply the operation
		err := toSession.Run(bson.M{"applyOps": opsToApply}, res)

		if err != nil {
			log.Logvf(log.DebugLow, "error: applying oplog %v ", opCount)
			printOplog(oplogEntry)
			missingcollection := strings.Contains(err.Error(), "missing collection")
			if missingcollection {
				log.Logvf(log.DebugLow, "error: `%v`", err.Error())
				log.Logvf(log.DebugLow, "creating collection  `%v`", oplogEntry.Namespace)
				collection := strings.Split(oplogEntry.Namespace, ".")[1]
				db := strings.Split(oplogEntry.Namespace, ".")[0]
				creation_err := toSession.DB(db).C(collection).Create(&mgo.CollectionInfo{})
				if creation_err != nil {
					return fmt.Errorf("error creating collection : %v", oplogEntry.Namespace)
				}
				err := toSession.Run(bson.M{"applyOps": opsToApply}, res)
				if err != nil {
					return fmt.Errorf("error applying ops: %v", err)
				}
			} else {
				return fmt.Errorf("error applying ops: %v", err)
			}
		}

		// check the server's response for an issue
		if !res.Ok {
			return fmt.Errorf("server gave error applying ops: %v", res.ErrMsg)
		}
	}

	// make sure there was no tailing error
	if err := tail.Err(); err != nil {
		return fmt.Errorf("error querying oplog: %v", err)
	}

	log.Logvf(log.DebugLow, "done applying %v oplog entries", opCount)

	return nil
}

// get the cursor for the oplog collection, based on the options
// passed in to mongooplog
func buildTailingCursor(oplog *mgo.Collection,
	sourceOptions *SourceOptions) *mgo.Iter {

	// how many seconds in the past we need
	secondsInPast := time.Duration(sourceOptions.Seconds) * time.Second
	// the time threshold for oplog queries
	threshold := time.Now().Add(-secondsInPast)
	// convert to a unix timestamp (seconds since epoch)
	thresholdAsUnix := threshold.Unix()

	// shift it appropriately, to prepare it to be converted to an
	// oplog timestamp
	thresholdShifted := uint64(thresholdAsUnix) << 32

	// build the oplog query
	oplogQuery := bson.M{
		"ts": bson.M{
			"$gte": bson.MongoTimestamp(thresholdShifted),
		},
	}

	// TODO: wait time
	// TODO: HSFT: make tailable optional
	// return oplog.Find(oplogQuery).Iter()
	return oplog.Find(oplogQuery).Tail(-1)

}
