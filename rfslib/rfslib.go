/*

This package specifies the application's interface to the distributed
records system (RFS) to be used in project 1 of UBC CS 416 2018W1.

You are not allowed to change this API, but you do have to implement
it.

*/

package rfslib

import (
	"errors"
	"fmt"
	"strings"

	//GOVEC
	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
)

// A Record is the unit of file access (reading/appending) in RFS.
type Record [512]byte

type IPArgs struct {
	LocalAddr string
	MinerID   string
}

type NameArgs struct {
	LocalAddr string
	Fname     string
}

type RecNumArgs struct {
	LocalAddr string
	Fname     string
	RecordNum uint16
}

type RecArgs struct {
	LocalAddr string
	Fname     string
	Record    *Record
}

// Reply Structs

type ClientJoinReply struct {
	Value bool
	Err   error
}

type CreateReply struct {
	Value string
	Err   error
}

type ListReply struct {
	Value []string
	Err   error
}

type TotalReply struct {
	Value uint16
	Err   error
}

type ReadReply struct {
	Value Record
	Err   error
}

type AppendReply struct {
	Value uint16
	Err   error
}

////////////////////////////////////////////////////////////////////////////////////////////
// Global Vars
var initialized bool
var logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
var options = govec.GetDefaultLogOptions()

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains minerAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("RFS: Disconnected from the miner [%s]", string(e))
}

// Contains filename. The *only* constraint on filenames in RFS is
// that must be at most 64 bytes long.
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("RFS: Filename [%s] has the wrong length", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("RFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// Contains filename
type FileExistsError string

func (e FileExistsError) Error() string {
	return fmt.Sprintf("RFS: Cannot create file with filename [%s] as it already exists", string(e))
}

// Contains filename
type FileMaxLenReachedError string

func (e FileMaxLenReachedError) Error() string {
	return fmt.Sprintf("RFS: File [%s] has reached its maximum length", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a connection to the RFS system.
type RFS interface {
	// Creates a new empty RFS file with name fname.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileExistsError
	// - BadFilenameError
	CreateFile(fname string) (err error)

	// Returns a slice of strings containing filenames of all the
	// existing files in RFS.
	//
	// Can return the following errors:
	// - DisconnectedError
	ListFiles() (fnames []string, err error)

	// Returns the total number of records in a file with filename
	// fname.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	TotalRecs(fname string) (numRecs uint16, err error)

	// Reads a record from file fname at position recordNum into
	// memory pointed to by record. Returns a non-nil error if the
	// read was unsuccessful. If a record at this index does not yet
	// exist, this call must block until the record at this index
	// exists, and then return the record.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	ReadRec(fname string, recordNum uint16, recordpointer *Record) (err error)

	// Appends a new record to a file with name fname with the
	// contents pointed to by record. Returns the position of the
	// record that was just appended as recordNum. Returns a non-nil
	// error if the operation was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	// - FileMaxLenReachedError
	AppendRec(fname string, record *Record) (recordNum uint16, err error)
}

// The constructor for a new RFS object instance. Takes the miner's
// IP:port address string as parameter, and the localAddr which is the
// local IP:port to use to establish the connection to the miner.
//
// The returned rfs instance is singleton: an application is expected
// to interact with just one rfs at a time.
//
// This call should only succeed if the connection to the miner
// succeeds. This call can return the following errors:
// - Networking errors related to localAddr or minerAddr
func Initialize(localAddr string, minerAddr string) (rfs RFS, err error) {
	//Remove carriage returns
	localAddr = strings.TrimSuffix(localAddr, "\r")
	minerAddr = strings.TrimSuffix(minerAddr, "\r")
	if !initialized {

		fmt.Println("Attempting to connect to miner with IP ", minerAddr)
		client, err := vrpc.RPCDial("tcp", minerAddr, logger, options)
		if err != nil {
			return nil, DisconnectedError(minerAddr)
		}
		if client != nil {
			defer client.Close()
		}

		args := IPArgs{localAddr, ""}

		var reply ClientJoinReply

		err = client.Call("MinerServices.Join", args, &reply)
		if err != nil {
			fmt.Println("Call Failed")
			return nil, DisconnectedError(minerAddr)
		}

		fmt.Println("RFS initialized")

		rfs := &rfslib{
			localAddr: localAddr,
			minerAddr: minerAddr,
		}

		return rfs, nil

	} else {
		return nil, errors.New("Client is already initialized")
	}
}

////////////////////////////////////////////////////////////////////////////////////////////
//RESPONSIBILITIES
//1. Allows a client to connect to a miner
//2. Allows a client to submit operations on files
//3. Operations uses record coins mined by the connected miner
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
//IMPLEMENTATION
////////////////////////////////////////////////////////////////////////////////////////////
type rfslib struct {
	localAddr string
	minerAddr string
}

func (rfs *rfslib) CreateFile(fname string) (err error) {

	// Can return the following errors:
	// - DisconnectedError
	// - FileExistsError
	// - BadFilenameError

	if len(fname) > 64 {
		return BadFilenameError(fname)
	}

	client, err := vrpc.RPCDial("tcp", rfs.minerAddr, logger, options)
	if err != nil {
		return DisconnectedError(rfs.minerAddr)
	}
	if client != nil {
		defer client.Close()
	}

	args := NameArgs{rfs.localAddr, fname}

	var reply CreateReply

	logger.LogLocalEvent("Calling CreateFile on Miner:"+rfs.minerAddr+" with file name: "+fname, govec.GetDefaultLogOptions())

	err = client.Call("MinerServices.CreateFile", args, &reply)
	if err != nil {
		return DisconnectedError(rfs.minerAddr)
	} else if reply.Err != nil {
		if reply.Err.Error() == "File Already Exists" {
			return FileExistsError(fname)
		} else if reply.Err.Error() == "Disconnected Miner" {
			return DisconnectedError(rfs.minerAddr)
		}
	}
	return nil
}

func (rfs *rfslib) ListFiles() (fnames []string, err error) {

	// Returns a slice of strings containing filenames of all the
	// existing files in RFS.
	//
	// Can return the following errors:
	// - DisconnectedError

	client, err := vrpc.RPCDial("tcp", rfs.minerAddr, logger, options)
	if err != nil {
		return nil, DisconnectedError(rfs.minerAddr)
	}
	if client != nil {
		defer client.Close()
	}

	args := IPArgs{rfs.localAddr, ""}

	var reply ListReply

	logger.LogLocalEvent("Calling ListFiles on Miner:"+rfs.minerAddr+" with file names", govec.GetDefaultLogOptions())

	err = client.Call("MinerServices.ListFiles", args, &reply)
	if err != nil {
		return nil, DisconnectedError(rfs.minerAddr)
	} else if reply.Err != nil {
		return nil, DisconnectedError(rfs.minerAddr)
	}

	return reply.Value, nil

}

func (rfs *rfslib) TotalRecs(fname string) (numRecs uint16, err error) {

	// Returns the total number of records in a file with filename
	// fname.
	//
	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError

	client, err := vrpc.RPCDial("tcp", rfs.minerAddr, logger, options)
	if err != nil {
		return 0, DisconnectedError(rfs.minerAddr)
	}
	if client != nil {
		defer client.Close()
	}

	args := NameArgs{rfs.localAddr, fname}

	var reply TotalReply

	logger.LogLocalEvent("Calling Total Recs on Miner:"+rfs.minerAddr+" for file name: "+fname, govec.GetDefaultLogOptions())

	err = client.Call("MinerServices.TotalRecs", args, &reply)
	if err != nil {
		return 0, DisconnectedError(rfs.minerAddr)
	} else {
		if reply.Err != nil {
			if reply.Err.Error() == "File Does Not Exist" {
				return 0, FileDoesNotExistError(fname)
			} else {
				return 0, DisconnectedError(rfs.minerAddr)
			}
		}
		return reply.Value, nil
	}
}

func (rfs *rfslib) ReadRec(fname string, recordNum uint16, recordpointer *Record) (err error) {

	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError

	client, err := vrpc.RPCDial("tcp", rfs.minerAddr, logger, options)
	if err != nil {
		return DisconnectedError(rfs.minerAddr)
	}
	if client != nil {
		defer client.Close()
	}

	args := RecNumArgs{rfs.localAddr, fname, recordNum}

	var reply ReadReply

	logger.LogLocalEvent("Calling ReadRec on Miner:"+rfs.minerAddr+" for file name: "+fname, govec.GetDefaultLogOptions())

	err = client.Call("MinerServices.ReadRec", args, &reply)
	if err != nil {
		return DisconnectedError(rfs.minerAddr)
	} else if reply.Err != nil {
		if reply.Err.Error() == "File Does Not Exist" {
			return FileDoesNotExistError(fname)
		} else {
			return DisconnectedError(rfs.minerAddr)
		}

	}

	copy(recordpointer[:], string(reply.Value[:len(reply.Value)]))
	return nil
}

func (rfs *rfslib) AppendRec(fname string, record *Record) (recordNum uint16, err error) {

	// Can return the following errors:
	// - DisconnectedError
	// - FileDoesNotExistError
	// - FileMaxLenReachedError

	client, err := vrpc.RPCDial("tcp", rfs.minerAddr, logger, options)
	if err != nil {
		return 0, DisconnectedError(rfs.minerAddr)
	}
	if client != nil {
		defer client.Close()
	}

	args := RecArgs{rfs.localAddr, fname, record}

	var reply AppendReply

	logger.LogLocalEvent("Calling AppendRec on Miner:"+rfs.minerAddr+" for file name: "+fname, govec.GetDefaultLogOptions())

	err = client.Call("MinerServices.AppendRec", args, &reply)
	if err != nil {
		return 0, DisconnectedError(rfs.minerAddr)

	} else if reply.Err != nil {
		if reply.Err.Error() == "File Does Not Exist" {
			return 0, FileDoesNotExistError(fname)
		} else if reply.Err.Error() == "File Max Len Reached" {
			return 0, FileMaxLenReachedError(fname)
		} else {
			return 0, DisconnectedError(rfs.minerAddr)
		}

	}
	return reply.Value, nil

}
