package malminerlib

import (
	"errors"
	"sync"
	"time"

	"encoding/json"
	"fmt"
	"strings"

	"crypto/md5"
	"encoding/hex"
	"github.com/DistributedClocks/GoVector/govec"
	"github.com/DistributedClocks/GoVector/govec/vrpc"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
)

//Represents a connection to a client via the RFSLib
type MinerLib interface {
	Start()
	JoinPeerMiner(peerIp string) error
	PublishOpToPeerMiners(op Operation) error
	PublishBlockToPeerMiner(peerAddr string, block Block, hashOfBlock string) error
	GetBlockChainFromPeerMiner(peerAddr string) (map[string]Block, error)

	//To facilitate testing
	GetPeerMiners() map[string]string
	GetConfig() Config
	GetOpsToDisseminate() []Operation
	GetOpQueue() []Operation
}

type minerlib struct {
}

////////////////////////////////////////////////////////////////////////////////////////////
//Responsibilities
////////////////////////////////////////////////////////////////////////////////////////////
//1. Disseminates ops from its clients and from other miners to other miners
//2. Mine record coins using proof of work
//Generates blocks for block chain, credits associated with its identifier
//3. Validates mined blocks and disseminates valid blocks from itself or from other miners
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
//IMPLEMENTATION
////////////////////////////////////////////////////////////////////////////////////////////
type OperationType string

const (
	CREATE OperationType = "CREATE"
	APPEND OperationType = "APPEND"
)

var miner *minerlib
var publicIpAddr string
var peers = map[string]string{}
var peersMutex = &sync.Mutex{}

var logger = govec.InitGoVector("client", "clientlogfile", govec.GetDefaultConfig())
var options = govec.GetDefaultLogOptions()

//Maintain a copy of the block chain
var CONFIG Config
var GENESIS Block
var RFSBLOCKCHAIN = make(map[string]Block)
var rfsmutex = &sync.Mutex{}

var createConfirmBlockHash string // Hash of latest confirmed block for create ops
var appendConfirmBlockHash string // Hash of latest confirmed block for append ops
var confirmMutex = &sync.Mutex{}

//var lastSeqNumber = 0 //First operation is 1
//var seqMutex = &sync.Mutex{}

//Ops from connected clients that will be moved to the operationsQueue when miner has enough coins for ops
var opsToDisseminateQueue []Operation
var opsToDisseminateMutex = &sync.Mutex{}

var operationsQueue []Operation
var opqmutex = &sync.Mutex{}

// var for aborting no op mining
var abort = false
var abortMutex = &sync.Mutex{}

var updateBeforeMining = false

type Record [512]byte

type Config struct {
	MinedCoinsPerOpBlock   int
	MinedCoinsPerNoOpBlock int
	NumCoinsPerFileCreate  int
	GenOpBlockTimeout      int
	GenesisBlockHash       string
	PowPerOpBlock          int
	PowPerNoOpBlock        int
	ConfirmsPerFileCreate  int
	ConfirmsPerFileAppend  int
	MinerID                string
	PeerMinersAddrs        []string
	IncomingMinersAddr     string
	OutgoingMinersIP       string
	IncomingClientsAddr    string
}

type Operation struct {
	TimeStamp time.Time
	//SeqNum    int //Order of operations for each miner
	Id        string
	Type      OperationType
	FileName  string // filename always req
	Data      Record // ignored in create
	MinerID   string
}

type Block struct {
	PrevHash     string
	Operations   []Operation
	BlockMinerID string
	Nonce        uint32
}

func Initialize(externalIpAddrWithoutPort string, configfilename string, delaySecondsUntilConnect int) (MinerLib, error) {
	if miner == nil {

		miner = &minerlib{}

		err := initConfig(configfilename)
		if err != nil {
			return nil, err
		}

		publicIpAddr = externalIpAddrWithoutPort + ":" + getMinerPort()
		//TODO: verify that the address is valid

		initGenesis()
		go RPCMinerServer()
		go RPCClientServer()

		fmt.Println("Initial delay to connect to peers for", delaySecondsUntilConnect, "seconds...")
		time.Sleep(time.Second * time.Duration(delaySecondsUntilConnect))

		initConnections()
		updateLocalBlockChainFromPeers()
		validateGenesis()

		return miner, nil
	} else {
		return nil, errors.New("Miner is already initialized.")
	}
}

func getMinerPort() string {
	parts := strings.Split(CONFIG.IncomingMinersAddr, ":")
	return parts[1]
}

func (miner *minerlib) Start() {
	//var requiresUpdate = false
	initConnections()
	//requiresUpdate = true
	fmt.Println("This miner does no work!")
	for true {
		if getNumPeers() > 0 {
			//if requiresUpdate {
				updateLocalBlockChainFromPeers()
			//}
			//mineNextBlock()
			//requiresUpdate = false
		}
	}

	/*for true {
		if getNumPeers() > 0 {
			if requiresUpdate {
				updateLocalBlockChainFromPeers()
			}
			mineNextBlock()
		} else {
			//Try joining peers listed in configuration
			initConnections()
			requiresUpdate = true
		}
	}*/
}

func updateLocalBlockChainFromPeers() {
	//fmt.Println("Updating block chain from peers")

	var longestBlockChainFromPeers map[string]Block
	var longestLength = 0
	for peerIp := range getPeers() {
		//fmt.Println("Attempting to reach ", peerIp)
		peerBlockChain, err := miner.GetBlockChainFromPeerMiner(peerIp)
		if err == nil {
			//fmt.Println("Reached ", peerIp)
			lenOfPeerBlockChain := len(peerBlockChain)
			if lenOfPeerBlockChain > longestLength {
				longestBlockChainFromPeers = peerBlockChain
				longestLength = lenOfPeerBlockChain
			}
		} else {
			fmt.Println(err.Error())
		}
	}

	if longestBlockChainFromPeers != nil {
		//fmt.Println("Updating Local Blockchain")
		updateBlockChain(longestBlockChainFromPeers)
	} else {
		fmt.Println("No peer blockchains were able to be retrieved.")
	}
}

func getPeers() map[string]string {
	peersMutex.Lock()
	defer peersMutex.Unlock()
	return peers
}

func updateLocalBlockChainFromPeer(peerIp string) {
	fmt.Println("Updating block chain from peer: ", peerIp)

	peerBlockChain, err := miner.GetBlockChainFromPeerMiner(peerIp)
	if err == nil {
		updateBlockChain(peerBlockChain)
	} else {
		fmt.Println("Unable to get block chain from peer ", peerIp, err.Error())
	}
}

/**
newBlockChain is a map of only the blocks on the longest chain
*/
func updateBlockChain(newBlockChain map[string]Block) {
	if newBlockChain != nil {
		//fmt.Println("Getting blocks on longest chain")
		_, blocks, _ := getBlocksOnLongestChain()
		//fmt.Println("Got blocks on longest chain")
		expectedGenesis := newBlockChain[CONFIG.GenesisBlockHash]

		//Only update if newBlockChain is longer than current chain and has the same genesis block
		if len(newBlockChain) > len(blocks) && expectedGenesis.PrevHash == "" {

			for blockHash, block := range newBlockChain {
				rfsmutex.Lock()
				if _, ok := RFSBLOCKCHAIN[blockHash]; !ok {
					RFSBLOCKCHAIN[blockHash] = block
				}
				rfsmutex.Unlock()
			}

		}
		setCreateFileConfirmBlockHash()
		setAppendFileConfirmBlockHash()
		//fmt.Println("Updating Local Seq Nums")
//		updateLocalSeqNums()
	}
}

/*func updateLocalSeqNums() {
	seqMutex.Lock()
	defer seqMutex.Unlock()

	opsToDisseminateMutex.Lock()
	defer opsToDisseminateMutex.Unlock()

	newLastSeqNum := getLastSeqNumOfMinerOnLongestChain(CONFIG.MinerID)

	//Align sequence number of pending ops from miner's clients (LOCAL) to new lastSeqNumber
	for _, op := range opsToDisseminateQueue {
		newLastSeqNum = newLastSeqNum + 1
		op.SeqNum = newLastSeqNum
	}

	//fmt.Println("Setting new seq Num")
	lastSeqNumber = newLastSeqNum
}*/

func initConnections() {
	for _, address := range CONFIG.PeerMinersAddrs {
		miner.JoinPeerMiner(address)
	}
}

func validateGenesis() {
	fmt.Println("Checking for genesis block..")
	if RFSBLOCKCHAIN != nil {
		_, exists := RFSBLOCKCHAIN[CONFIG.GenesisBlockHash]
		if !exists {
			fmt.Println("Making Genesis Block")
			initGenesis()
		}
	} else {
		initGenesis()
	}
}

func initConfig(configfilename string) error {
	//Read config.json file, read config values into Config type
	configJson, err := os.Open(configfilename)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer configJson.Close()
	byteValue, _ := ioutil.ReadAll(configJson)

	json.Unmarshal(byteValue, &CONFIG)
	return nil
}

func initGenesis() {
	// Generate GENESIS block from config hash

	genhash := CONFIG.GenesisBlockHash

	var nonce uint32 = 0
	var genblock = Block{"", []Operation{}, CONFIG.MinerID, nonce}

	rfsmutex.Lock()
	RFSBLOCKCHAIN[genhash] = genblock
	rfsmutex.Unlock()

	GENESIS = genblock

	fmt.Println("Genesis block with hash ", genhash, " and nonce ", GENESIS.Nonce, " formed")

}

func (miner *minerlib) JoinPeerMiner(peerAddr string) error {
	fmt.Println("Attempting to join: ", peerAddr)
	peer, err := vrpc.RPCDial("tcp", peerAddr, logger, options)
	if err != nil {
		fmt.Println("Failed to join: ", peerAddr, " not accepting connections", err)
		return err
	} else {
		if peer != nil {
			defer peer.Close()
		}
		args := JoinArgs{publicIpAddr, CONFIG.MinerID}
		var reply JoinReply
		err = peer.Call("PeerMinerServices.Join", args, &reply)
		if err != nil {
			fmt.Println("Failed to join: ", peerAddr, err)
			return err
		} else {
			peersMutex.Lock()
			peers[peerAddr] = reply.MinerID
			peersMutex.Unlock()

			fmt.Println("Joined as peer miner of: ", peerAddr)
			updateBlockChain(reply.LongestBlockChainAsMap)
			return nil
		}
	}
}

func (miner *minerlib) PublishBlockToPeerMiner(peerAddr string, block Block, hashOfBlock string) error {
	peer, err := vrpc.RPCDial("tcp", peerAddr, logger, options)
	if err != nil {
		fmt.Println("Failed to connect to: ", peerAddr, " not accepting connections", err)
		removePeer(peerAddr)
		return err
	} else {
		if peer != nil {
			defer peer.Close()
		}
		args := BlockArgs{publicIpAddr, hashOfBlock, block}
		var isValidBlock bool
		err = peer.Call("PeerMinerServices.PublishBlock", args, &isValidBlock)
		if err != nil {
			fmt.Println("Failed to send block: ", peerAddr, err)
			return err
		}
		if !isValidBlock {
			updateLocalBlockChainFromPeer(peerAddr)
			//updateBeforeMining = true
		}
		return nil
	}
}

func (miner *minerlib) GetBlockChainFromPeerMiner(peerAddr string) (map[string]Block, error) {
	//fmt.Println("Attempting to connect to: ", peerAddr)
	peer, err := vrpc.RPCDial("tcp", peerAddr, logger, options)
	if err != nil {
		fmt.Println("Failed to connect to: ", peerAddr, " not accepting connections", err)
		removePeer(peerAddr)
		return nil, err
	} else {
		if peer != nil {
			defer peer.Close()
		}
		args := AddrArgs{publicIpAddr}
		var reply BlockChainReply
		err = peer.Call("PeerMinerServices.GetBlockChain", args, &reply)
		if err != nil {
			fmt.Println("Failed to get blockchain from peer: ", peerAddr, err)
			removePeer(peerAddr)
			return nil, err
		} else {
			if reply.LongestBlockChainAsMap == nil || reply.LongestBlockChainAsMap[CONFIG.GenesisBlockHash].PrevHash != "" {
				return nil, errors.New("invalid/incompatible blockchain from peer miner")
			}
			return reply.LongestBlockChainAsMap, nil
		}
	}
}

func removePeer(peerAddr string) {
	peersMutex.Lock()
	defer peersMutex.Unlock()
	delete(peers, peerAddr)
	fmt.Println("Removed peer: " + peerAddr)
}

/**
Validates that the coin balance never drops below 0 on any point of the longest chain
or the given chain is using hashToValidateFrom when validateOnLongestChain is set to true
*/
func GetRecordCoinBalance(minerId string, validateOnLongestChain bool, hashToValidateFrom string) (int, error) {
	var blocksOnChain []Block
	var lastBlockHash string

	if validateOnLongestChain == false && hashToValidateFrom != "" {
		lastBlockHash = hashToValidateFrom
	} else {
		lastBlockHashOnLongestChain, _, _ := getBlocksOnLongestChain()
		lastBlockHash = lastBlockHashOnLongestChain
	}

	nextBlockHashToRetrieve := lastBlockHash
	visitedHashes := make(map[string]int)

	//Traverse up the chain until we reach the genesis block
	for nextBlockHashToRetrieve != "" {
		if visitedHashes[nextBlockHashToRetrieve] == 1 {
			updateLocalBlockChainFromPeers()
			return 0, errors.New("Duplicate block detected, could be caused a loop in the chain. Blockchain has been updated from peers.")
		}
		rfsmutex.Lock()
		nextBlock := RFSBLOCKCHAIN[nextBlockHashToRetrieve]
		rfsmutex.Unlock()

		visitedHashes[nextBlockHashToRetrieve] = 1

		blocksOnChain = append(blocksOnChain, nextBlock)
		nextBlockHashToRetrieve = nextBlock.PrevHash
	}

	lengthOfChain := len(blocksOnChain)
	result := 0

	//Skip block 0, assuming genesis block does not have any ops
	for i := lengthOfChain - 2; i >= 0; i-- {
		currBlock := blocksOnChain[i]
		currBlockOps := currBlock.Operations

		for _, op := range currBlockOps {
			if op.MinerID == minerId {
				result = result - operationCost(op)
			}
			if result < 0 {
				updateLocalBlockChainFromPeers()
				return 0, errors.New("Coin balance went below 0. OpId: " + op.Id)
			}
		}

		if currBlock.BlockMinerID == minerId {
			if len(currBlockOps) > 0 {
				result = result + CONFIG.MinedCoinsPerOpBlock
			} else {
				result = result + CONFIG.MinedCoinsPerNoOpBlock
			}

		}
	}

	return result, nil
}

func operationCost(op Operation) int {
	cost := 0
	opType := op.Type
	if opType == CREATE {
		cost = CONFIG.NumCoinsPerFileCreate
	} else if opType == APPEND {
		cost = 1
	}
	return cost
}

func mineNextBlock() {
	//Disseminate any client ops that are now affordable
	if updateBeforeMining {
		updateLocalBlockChainFromPeers()
		//fmt.Println("Updated Local BlockChain")
		setAppendFileConfirmBlockHash()
		setCreateFileConfirmBlockHash()
		//updateLocalSeqNums()
		//fmt.Println("Updated Local Seq Nums")
	}
	disseminateContinguouslyAffordableOperations()
	fmt.Println("Opq has ", len(operationsQueue), " things")

	updateBeforeMining = false

	if len(operationsQueue) == 0 {
		mineNoOpBlock()
	} else {
		time.Sleep(time.Duration(CONFIG.GenOpBlockTimeout) * time.Millisecond)
		mineOpBlock()
	}
}

func mineNoOpBlock() {
	lastblock, _, _ := getBlocksOnLongestChain()
	fmt.Println("Mining no op block pointing to ", lastblock)
	block := Block{lastblock, []Operation{}, CONFIG.MinerID, 0}
	go abortUponOp()
	mineBlock(block, false)
}

func abortUponOp() {
	for true {
		time.Sleep(20 * time.Millisecond)
		opqmutex.Lock()
		//opqmutex.Unlock()
		//fmt.Println("OpQ locked by abortUponOp")
		if len(operationsQueue) > 0 {
			abortMutex.Lock()
			abort = true
			abortMutex.Unlock()
			opqmutex.Unlock()
			break
		}
		//fmt.Println("OpQ unlocking by abortUponOp")
		opqmutex.Unlock()

	}

}

func mineOpBlock() {
	lastblock, _, _ := getBlocksOnLongestChain()
	fmt.Println("Mining op block pointing to ", lastblock)
	var blockOps []Operation
	//fmt.Println("Waiting for opqmutex")
	opqmutex.Lock()

	numOps := len(operationsQueue)
	//fmt.Println("OpQ Locked")
	//Keep track of last seqNum of miners as we add ops to block
	//seqNumOfMiners := make(map[string]int)

	newOpsQueue := operationsQueue
	var skippedOps []Operation
	//fmt.Println("Adding Ops")
	//queueindex := 0
	for i := 0; i < numOps; i++ {
		if len(blockOps) == 100 {
			break
		}

		nextOpToAdd := operationsQueue[0]
		// Remove from opq while evaluating
		operationsQueue = operationsQueue[:0+copy(operationsQueue[0:], operationsQueue[0+1:])]

		/*seqNumOfMiner := seqNumOfMiners[nextOpToAdd.MinerID]
		if seqNumOfMiner == 0 {
			seqNumOfMiner = getLastSeqNumOfMinerOnLongestChain(nextOpToAdd.MinerID)
		}*/

		//Only add to block only if it is the next op in sequence for the miner
		/*if seqNumOfMiner+1 == nextOpToAdd.SeqNum {
			createExists := false
			for _, op := range blockOps {
				if op.Type == "CREATE" && op.FileName == nextOpToAdd.FileName {
					createExists = true
				}
			}
			if !createExists {
				blockOps = append(blockOps, nextOpToAdd)
				seqNumOfMiner = nextOpToAdd.SeqNum
			}
		} else if seqNumOfMiner < nextOpToAdd.SeqNum {
			skippedOps = append(skippedOps, nextOpToAdd)
		} else {
			fmt.Println("Op has completed seqNum <= current seq num of miner", nextOpToAdd)
			if !isOpOnLongestChain(nextOpToAdd.Id) {
				fmt.Println("Op is not on longest chain, add back to queue", nextOpToAdd)
				skippedOps = append(skippedOps, nextOpToAdd)
			}
		}*/

		//seqNumOfMiners[nextOpToAdd.MinerID] = seqNumOfMiner
		createExists := false
			for _, op := range blockOps {
				if op.Type == "CREATE" && op.FileName == nextOpToAdd.FileName {
					createExists = true
				}
			}
			if !createExists {
				blockOps = append(blockOps, nextOpToAdd)
				
			}
		newOpsQueue = newOpsQueue[1:]
	}

	operationsQueue = newOpsQueue
	//fmt.Println("Unlocked OpQ")
	opqmutex.Unlock()

	for _, skippedOp := range skippedOps {
		fmt.Println("Skipped:")
		printOpDetails(skippedOp)
		//fmt.Println("with seq num: ", skippedOp.SeqNum)
		appendToOperationsQueue(skippedOp)
	}
	fmt.Println("Ops in Block:")
	for _, op := range blockOps {
		printOpDetails(op)
	}
	block := Block{lastblock, blockOps, CONFIG.MinerID, 0}
	//fmt.Println("Calling PoW")
	mineBlock(block, true)
}

/*func isOpOnLongestChain(opId string) bool {
	result := false
	_, blocks, _ := getBlocksOnLongestChain()
	for _, block := range blocks {
		if blockHasOperationId(block, opId) {
			result = true
			break
		}
	}

	return result
}*/

// Proof of work algorithm to determine nonce of block based on config difficulty

func mineBlock(block Block, op bool) {
	var nonce uint32 = 0
	block.Nonce = nonce
	inithash := md5.New()
	var initblockbytes []byte
	initblockbytes, err := json.Marshal(block)
	if err != nil {
		fmt.Println("error: ", err)
	}
	inithash.Write(initblockbytes)
	hashstr := hex.EncodeToString(inithash.Sum(nil))

	var hash = hashstr

	var zeros = ""
	var numzeros = 0
	if op {
		zeros = strings.Repeat("0", CONFIG.PowPerOpBlock)
		numzeros = CONFIG.PowPerOpBlock
	} else {
		zeros = strings.Repeat("0", CONFIG.PowPerNoOpBlock)
		numzeros = CONFIG.PowPerNoOpBlock
	}
	done := false
	//fmt.Println(string(hash[len(hash)-1-CONFIG.PowPerNoOpBlock]))
	noopabort := false

	for (string(hash[len(hash)-numzeros:]) != zeros) && (!noopabort) {

		//fmt.Println("last",CONFIG.PowPerNoOpBlock,"chars of hash: ", hash[len(hash)-CONFIG.PowPerNoOpBlock:])
		block.Nonce = nonce

		h := md5.New()
		var blockbytes []byte

		blockbytes, err := json.Marshal(block)
		if err != nil {
			fmt.Println("error: ", err)
		}

		h.Write(blockbytes)
		str := hex.EncodeToString(h.Sum(nil))
		hash = str

		//fmt.Println("nonce: ", block.Nonce, " hash: ", hash)
		if string(hash[len(hash)-numzeros:]) == zeros {
			done = true
		}
		//fmt.Println(nonce)
		nonce++

		if !op {
			abortMutex.Lock()
			noopabort = abort
			abortMutex.Unlock()
		}

	}
	if done {

		fmt.Println("Validating mined block")
		valid, opsToRetry := validateMinedBlock(block)

		if valid {
			fmt.Println("Mined block is valid")
			rfsmutex.Lock()
			RFSBLOCKCHAIN[hash] = block
			rfsmutex.Unlock()

			setCreateFileConfirmBlockHash()
			setAppendFileConfirmBlockHash()
			fmt.Println("Final nonce: ", block.Nonce, " hash: ", hash)

			for peerIp := range getPeers() {
				miner.PublishBlockToPeerMiner(peerIp, block, hash)
			}
		} else {
			// Don't add the block to the chain. Add valid ops back to the queue to retry.
			retryOps(opsToRetry)
		}
	} else {
		fmt.Println("Op came in! Aborting no op mining.")
		abortMutex.Lock()
		abort = false
		abortMutex.Unlock()
	}
}

// Returns hash of last block on longest chain in RFSBLOCKCHAIN
// Block slice is for longest chain
// Map only contains blocks on the longest chain
func getBlocksOnLongestChain() (string, []Block, map[string]Block) {

	//fmt.Println("Finding Last Block..")

	var longestlength = 0
	var lastblockhash = ""
	blocksOnLongestChain := []Block{}
	mapOfBlocksOnLongestChain := make(map[string]Block)
	//fmt.Println("longest block waiting for rfsmutex")
	rfsmutex.Lock()
	//fmt.Println("longest block locking rfsmutex")
	for hash := range RFSBLOCKCHAIN {
		rfsmutex.Unlock()
		//fmt.Println("Checking ", hash, " for length")
		var currentblockHash = hash
		var length = 1
		blocksOnChain := []Block{}
		var blocksMap = make(map[string]Block)


		for currentblockHash != CONFIG.GenesisBlockHash {
			rfsmutex.Lock()
			currentBlock := RFSBLOCKCHAIN[currentblockHash]
			rfsmutex.Unlock()
			blocksOnChain = append(blocksOnChain, currentBlock)
			blocksMap[currentblockHash] = currentBlock
			currentblockHash = currentBlock.PrevHash
			length++
		}

		//fmt.Println("length is ", length)
		if length > longestlength {
			longestlength = length
			lastblockhash = hash

			//Add Genesis block
			rfsmutex.Lock()
			genesisBlock := RFSBLOCKCHAIN[CONFIG.GenesisBlockHash]
			rfsmutex.Unlock()

			blocksOnLongestChain = append(blocksOnLongestChain, genesisBlock)
			blocksMap[CONFIG.GenesisBlockHash] = genesisBlock

			blocksOnLongestChain = blocksOnChain
			mapOfBlocksOnLongestChain = blocksMap
		}
		rfsmutex.Lock()
	}
	//fmt.Println("longest block unlocking rfsmutex")
	rfsmutex.Unlock()

	//fmt.Println("Latest Block is: ", lastblockhash)
	return lastblockhash, blocksOnLongestChain, mapOfBlocksOnLongestChain

}

// Sets createConfirmBlockHash to hash of latest block that confirms create ops
func setCreateFileConfirmBlockHash() {

	lastblock, _, _ := getBlocksOnLongestChain()
	confirmblock := lastblock
	for i := 0; i < CONFIG.ConfirmsPerFileCreate; i++ {

		if confirmblock != CONFIG.GenesisBlockHash {
			rfsmutex.Lock()
			confirmblock = RFSBLOCKCHAIN[lastblock].PrevHash
			rfsmutex.Unlock()
		} else {
			confirmMutex.Lock()
			createConfirmBlockHash = ""
			confirmMutex.Unlock()
		}

	}

	confirmMutex.Lock()
	createConfirmBlockHash = confirmblock
	confirmMutex.Unlock()
	//fmt.Println("Confirmed Block (create):" + createConfirmBlockHash)

}

// Sets appendConfirmBlockHash to hash of latest block that confirms append ops
func setAppendFileConfirmBlockHash() {

	lastblock, _, _ := getBlocksOnLongestChain()
	confirmblock := lastblock
	for i := 0; i < CONFIG.ConfirmsPerFileAppend; i++ {

		if confirmblock != CONFIG.GenesisBlockHash {
			rfsmutex.Lock()
			confirmblock = RFSBLOCKCHAIN[lastblock].PrevHash
			rfsmutex.Unlock()
		} else {
			confirmMutex.Lock()
			appendConfirmBlockHash = ""
			confirmMutex.Unlock()
		}

	}

	confirmMutex.Lock()
	appendConfirmBlockHash = confirmblock
	confirmMutex.Unlock()
	//fmt.Println("Confirmed Block (append):" + appendConfirmBlockHash)
}

//////////////
//RPC Calls
//////////////

type MinerServices struct {
}

type PeerMinerServices struct {
}

/////////////////////
// RPC ARG STRUCTS
/////////////////////

type JoinArgs struct {
	PublicAddr string
	MinerID    string
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
	Record    Record
}

type OpArgs struct {
	PublicAddr string
	Op         Operation
}

type BlockArgs struct {
	PublicAddr string
	Hash       string
	Block      Block
}

type AddrArgs struct {
	PublicAddr string
}

/////////////////////
// RPC REPLY STRUCTS
/////////////////////

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

type JoinReply struct {
	MinerID                string
	LongestBlockChainAsMap map[string]Block
}

type BlockChainReply struct {
	LongestBlockChainAsMap map[string]Block // # keys == len(map) == length of chain
}

//For Clients

var connectedClients []string

// Start RPC server for Client-Miner Calls

func RPCClientServer() {
	fmt.Println("Starting Client RPC server")
	logger := govec.InitGoVector("server", "serverlogfile", govec.GetDefaultConfig())
	minerServices := new(MinerServices)
	server := rpc.NewServer()
	server.Register(minerServices)

	localAddrBindForClient := CONFIG.IncomingClientsAddr

	fmt.Println("Listening on ", localAddrBindForClient, " for client calls")
	l, e := net.Listen("tcp", localAddrBindForClient)
	//defer l.Close()
	if e != nil {
		log.Fatal("listen error:", e)
	}
	options := govec.GetDefaultLogOptions()
	vrpc.ServeRPCConn(server, l, logger, options)
}

func (t *MinerServices) Join(args *JoinArgs, reply *ClientJoinReply) error {
	fmt.Println(args.PublicAddr, "joined as client!")
	connectedClients = append(connectedClients, args.PublicAddr)
	*reply = ClientJoinReply{true, nil}
	return nil
}

/*func getNextSeqNumber() int {
	seqMutex.Lock()
	lastSeqNumber = lastSeqNumber + 1
	nextSeqNum := lastSeqNumber
	seqMutex.Unlock()

	return nextSeqNum
}*/

func (t *MinerServices) CreateFile(args *NameArgs, reply *CreateReply) error {
	name := args.Fname

	if len(peers) == 0 {
		*reply = CreateReply{name, errors.New("Disconnected Miner")}
		return nil
	}

	// Check if file exists
	setAppendFileConfirmBlockHash()
	setCreateFileConfirmBlockHash()

	current := ""
	exists := false

	if CONFIG.ConfirmsPerFileAppend <= CONFIG.ConfirmsPerFileCreate {
		confirmMutex.Lock()
		current = appendConfirmBlockHash
		confirmMutex.Unlock()
	} else {
		confirmMutex.Lock()
		current = createConfirmBlockHash
		confirmMutex.Unlock()
	}
	rfsmutex.Lock()
	for current != CONFIG.GenesisBlockHash && !exists {
		for _, operation := range RFSBLOCKCHAIN[current].Operations {
			if operation.FileName == name {
				exists = true
				rfsmutex.Unlock()
				*reply = CreateReply{name, errors.New("File Already Exists")}
				return nil
			}
		}
		current = RFSBLOCKCHAIN[current].PrevHash
	}
	rfsmutex.Unlock()

	if !exists {

		largestValue := 0
		richestMiner := ""

		// Maliciously sets miner ID to miner that has most coins and disseminates create op:
		for largestValue - CONFIG.NumCoinsPerFileCreate < 0 {
			// Calculates balance for each miner in longest chain
			walletForMiners := make(map[string]int)
			lastHash, _, longestBlockChain := getBlocksOnLongestChain()
			current := lastHash
			for current != CONFIG.GenesisBlockHash {
				isNoOpBlock := true

				for _, op := range longestBlockChain[current].Operations {
					walletForMiners[op.MinerID] = walletForMiners[op.MinerID] - operationCost(op)
					isNoOpBlock = false
				}

				if isNoOpBlock {
					walletForMiners[longestBlockChain[current].BlockMinerID] += CONFIG.MinedCoinsPerNoOpBlock
				} else {
					walletForMiners[longestBlockChain[current].BlockMinerID] += CONFIG.MinedCoinsPerOpBlock
				}

				current = longestBlockChain[current].PrevHash
			}

			largestValue = 0
			richestMiner = ""

			for miner, value := range walletForMiners{
				if value > largestValue{
					richestMiner = miner
					largestValue = value
				}
			}
		}




		// Sets MinerID to richest miner

		//spoofseqnumber := getLastSeqNumOfMinerOnLongestChain(richestMiner) + 1

		opWithoutUniqueId := Operation{TimeStamp: time.Now(), Type: "CREATE", FileName: name, MinerID: richestMiner}
		op, err := setOpID(opWithoutUniqueId)
		if err != nil {
			return err
		}

		//addtoOpsToDisseminateQueue(op)
		//disseminateContinguouslyAffordableOperations()
		fmt.Println("Creating Create op by spoofing Miner ID: " , richestMiner, " because they have the most coins! (",largestValue," coins)")
		appendToOperationsQueue(op)
		miner.PublishOpToPeerMiners(op)

		confirmed := false
		for !confirmed {
			if len(peers) == 0 {
				*reply = CreateReply{name, errors.New("Disconnected Miner")}
				return nil
			}

			confirmMutex.Lock()
			currentblock := createConfirmBlockHash
			confirmMutex.Unlock()

			for currentblock != CONFIG.GenesisBlockHash {
				rfsmutex.Lock()
				for _, operation := range RFSBLOCKCHAIN[currentblock].Operations {
					if operation.Type == "CREATE" && operation.FileName == name {
						if op.Id != operation.Id {
							//Someone else created the file concurrently and beat you to it
							*reply = CreateReply{name, errors.New("File Already Exists")}
							rfsmutex.Unlock()
							return nil
						}
						confirmed = true
					}
				}
				currentblock = RFSBLOCKCHAIN[currentblock].PrevHash
				rfsmutex.Unlock()
			}

		}
		*reply = CreateReply{name, nil}
		return nil
	} else {
		*reply = CreateReply{name, errors.New("File Already Exists")}
		return nil
	}

}

func addtoOpsToDisseminateQueue(op Operation) {
	opsToDisseminateMutex.Lock()
	opsToDisseminateQueue = append(opsToDisseminateQueue, op)
	opsToDisseminateMutex.Unlock()
}

/**
Adds the first n operations that the miner can afford in the opsToDisseminateQueue to the operationsQueue
*/
func disseminateContinguouslyAffordableOperations() error {
	if getNumPeers() > 0 {
		currentBalance, err := GetRecordCoinBalance(CONFIG.MinerID, true, "")
		if err != nil {
			return err
		}

		if currentBalance > 0 {
			opsToDisseminateMutex.Lock()
			defer opsToDisseminateMutex.Unlock()

			opsToDisseminateQueueCopy := opsToDisseminateQueue

			for _, clientOp := range opsToDisseminateQueueCopy {
				currentBalance -= operationCost(clientOp)
				if currentBalance >= 0 {

					appendToOperationsQueue(clientOp)

					opsToDisseminateQueue = opsToDisseminateQueue[1:]
					miner.PublishOpToPeerMiners(clientOp)
				}
			}

		}
	} else {
		fmt.Println("No peers to disseminate ops.")
	}

	return nil
}

func setOpID(operation Operation) (Operation, error) {
	md5Hash := md5.New()
	operationBytes, err := json.Marshal(operation)
	if err != nil {
		return Operation{}, errors.New("Error - setOpID. Marashalling operation. " + err.Error())
	}

	md5Hash.Write(operationBytes)
	hashstr := hex.EncodeToString(md5Hash.Sum(nil))
	operation.Id = hashstr

	return operation, nil
}

// Finds all create ops in confirmed create blockchain and returns to client
func (t *MinerServices) ListFiles(args *JoinArgs, reply *ListReply) error {

	fmt.Println(args.PublicAddr, "called ListFiles!")
	var files []string = []string{}
	if len(peers) == 0 {
		*reply = ListReply{files, errors.New("Disconnected Miner")}
		return nil
	}

	setCreateFileConfirmBlockHash()
	confirmMutex.Lock()
	current := createConfirmBlockHash
	confirmMutex.Unlock()

	rfsmutex.Lock()
	for current != CONFIG.GenesisBlockHash {
		for _, operation := range RFSBLOCKCHAIN[current].Operations {
			if operation.Type == "CREATE" {
				files = append(files, operation.FileName)
				fmt.Println("File name: ", operation.FileName, " worker: ", operation.MinerID, " found on block: ", current)
			}
		}
		current = RFSBLOCKCHAIN[current].PrevHash
	}
	rfsmutex.Unlock()
	filesString, _ := json.Marshal(files)
	fmt.Println("Replying with: ", string(filesString))
	*reply = ListReply{files, nil}

	return nil
}

//Returns num of recs in file with args.fname
func (t *MinerServices) TotalRecs(args *NameArgs, reply *TotalReply) error {

	fmt.Println(args.LocalAddr, "called TotalRecs!")

	if len(peers) == 0 {
		*reply = TotalReply{0, errors.New("Disconnected Miner")}
		return nil
	}

	var numrecs uint16 = 0
	confirmMutex.Lock()
	current := appendConfirmBlockHash
	confirmMutex.Unlock()
	rfsmutex.Lock()
	defer rfsmutex.Unlock()
	for current != CONFIG.GenesisBlockHash {
		for i := len(RFSBLOCKCHAIN[current].Operations) - 1; i >= 0; i-- {
			if RFSBLOCKCHAIN[current].Operations[i].Type == "APPEND" && RFSBLOCKCHAIN[current].Operations[i].FileName == args.Fname {
				numrecs++
			}
			if RFSBLOCKCHAIN[current].Operations[i].Type == "CREATE" && RFSBLOCKCHAIN[current].Operations[i].FileName == args.Fname {
				*reply = TotalReply{numrecs, nil}
				return nil
			}
		}
		current = RFSBLOCKCHAIN[current].PrevHash
	}

	*reply = TotalReply{0, nil}
	return nil

}

func (t *MinerServices) ReadRec(args *RecNumArgs, reply *ReadReply) error {

	fmt.Println(args.LocalAddr, "called ReadRec!")

	if len(peers) == 0 {
		*reply = ReadReply{Record{}, errors.New("Disconnected Miner")}
		return nil
	}

	var fnameRecords []Record = []Record{}
	exists := false

	for args.RecordNum+1 > uint16(len(fnameRecords)) {
		//fnameRecords = []Record{}
		confirmMutex.Lock()
		current := appendConfirmBlockHash
		confirmMutex.Unlock()
		for current != CONFIG.GenesisBlockHash {

			rfsmutex.Lock()
			numOps := len(RFSBLOCKCHAIN[current].Operations)
			rfsmutex.Unlock()

			for i := numOps - 1; i >= 0; i-- {
				rfsmutex.Lock()
				if RFSBLOCKCHAIN[current].Operations[i].Type == "APPEND" && RFSBLOCKCHAIN[current].Operations[i].FileName == args.Fname {
					fnameRecords = append(fnameRecords, RFSBLOCKCHAIN[current].Operations[i].Data)
					//exists = true
				} else if RFSBLOCKCHAIN[current].Operations[i].Type == "CREATE" && RFSBLOCKCHAIN[current].Operations[i].FileName == args.Fname {
					//rfsmutex.Unlock()
					/*if !(args.RecordNum >= uint16(len(fnameRecords))) {
						fmt.Println("Replying with: ", fnameRecords[(uint16(len(fnameRecords)) - 1 - args.RecordNum)])
						*reply = ReadReply{fnameRecords[(uint16(len(fnameRecords)) - 1 - args.RecordNum)], nil}
						rfsmutex.Unlock()
						return nil
					}*/
					exists = true

				}
				rfsmutex.Unlock()
				if exists {
					break
				}
			}
			rfsmutex.Lock()
			current = RFSBLOCKCHAIN[current].PrevHash
			rfsmutex.Unlock()
			if exists {
				break
			}

			if current == CONFIG.GenesisBlockHash && !exists {

				*reply = ReadReply{Record{}, errors.New("File Does Not Exist")}
				return nil
			}
		}
	}
	fmt.Println(len(fnameRecords))
	*reply = ReadReply{fnameRecords[(uint16(len(fnameRecords)) - 1 - args.RecordNum)], nil}
	return nil
}

func (t *MinerServices) AppendRec(args *RecArgs, reply *AppendReply) error {
	fmt.Println(args.LocalAddr, "called AppendRec!")
	name := args.Fname

	if len(peers) == 0 {
		*reply = AppendReply{0, errors.New("Disconnected Miner")}
		return nil
	}

	// Check if file exists
	setAppendFileConfirmBlockHash()
	setCreateFileConfirmBlockHash()

	current := ""
	exists := false
	if CONFIG.ConfirmsPerFileAppend <= CONFIG.ConfirmsPerFileCreate {
		confirmMutex.Lock()
		current = appendConfirmBlockHash
		confirmMutex.Unlock()
	} else {
		confirmMutex.Lock()
		current = createConfirmBlockHash
		confirmMutex.Unlock()
	}

	for current != CONFIG.GenesisBlockHash && !exists {
		rfsmutex.Lock()
		for _, operation := range RFSBLOCKCHAIN[current].Operations {
			if operation.FileName == name {
				exists = true
			}
		}
		current = RFSBLOCKCHAIN[current].PrevHash
		rfsmutex.Unlock()
	}

	if exists {


		largestValue := 0
		richestMiner := ""

		// Maliciously sets miner ID to miner that has most coins and disseminates create op:
		for largestValue - CONFIG.NumCoinsPerFileCreate < 0 {
			// Calculates balance for each miner in longest chain
			lastHash, _, longestBlockChain := getBlocksOnLongestChain()
			walletForMiners := make(map[string]int)
			current := lastHash
			for current != CONFIG.GenesisBlockHash {
				isNoOpBlock := true

				for _, op := range longestBlockChain[current].Operations {
					walletForMiners[op.MinerID] = walletForMiners[op.MinerID] - operationCost(op)
					isNoOpBlock = false
				}

				if isNoOpBlock {
					walletForMiners[longestBlockChain[current].BlockMinerID] += CONFIG.MinedCoinsPerNoOpBlock
				} else {
					walletForMiners[longestBlockChain[current].BlockMinerID] += CONFIG.MinedCoinsPerOpBlock
				}

				current = longestBlockChain[current].PrevHash
			}

			largestValue = 0
			richestMiner = ""

			for miner, value := range walletForMiners{
				if value > largestValue{
					richestMiner = miner
					largestValue = value
				}
			}
		}


		fmt.Println("Creating Append op by spoofing Miner ID: " , richestMiner, " because they have the most coins! (",largestValue," coins)")

		

		// Count number of appends to not go over max appends #
		var numAppends uint16 = 0
		for current != CONFIG.GenesisBlockHash && !exists {
			rfsmutex.Lock()
			for _, operation := range RFSBLOCKCHAIN[current].Operations {
				if operation.Type == "APPEND" && operation.FileName == name {
					numAppends++
				}
			}
			current = RFSBLOCKCHAIN[current].PrevHash
			rfsmutex.Unlock()
		}
		if numAppends >= 65535 {
			*reply = AppendReply{0, errors.New("File Max Len Reached")}
			return nil
		}

		// Sets MinerID to richest miner

		//spoofseqnumber := getLastSeqNumOfMinerOnLongestChain(richestMiner) + 1

		op := Operation{TimeStamp: time.Now(), Type: "APPEND", FileName: name, Data: args.Record, MinerID: richestMiner}
		op, err := setOpID(op)
		if err != nil {
			fmt.Println(err.Error())
		}

		appendToOperationsQueue(op)
		miner.PublishOpToPeerMiners(op)

		if len(peers) == 0 {
			*reply = AppendReply{0, errors.New("Disconnected Miner")}
			return nil
		}

		// Block until confirmed
		confirmed := false

		operationBlock := ""
		for !confirmed {
			setAppendFileConfirmBlockHash()
			setCreateFileConfirmBlockHash()
			if len(peers) == 0 {
				*reply = AppendReply{0, errors.New("Disconnected Miner")}
				return nil
			}
			confirmMutex.Lock()
			currentblock := appendConfirmBlockHash
			confirmMutex.Unlock()
			for currentblock != CONFIG.GenesisBlockHash {
				rfsmutex.Lock()
				for _, operation := range RFSBLOCKCHAIN[currentblock].Operations {
					if operation.Id == op.Id {
						//fmt.Println("Type: ", operation.OpType, " Fname: ", operation.Fname)
						confirmed = true
						operationBlock = currentblock
					}
				}

				currentblock = RFSBLOCKCHAIN[currentblock].PrevHash
				rfsmutex.Unlock()
			}

		}

		// Find index of append op

		current := operationBlock
		var index uint16 = 0
		startcounting := false

		//defer rfsmutex.Unlock()
		for current != CONFIG.GenesisBlockHash {
			rfsmutex.Lock()
			for i := len(RFSBLOCKCHAIN[current].Operations) - 1; i >= 0; i-- {

				if RFSBLOCKCHAIN[current].Operations[i] == op {
					startcounting = true
					//index++
				} else if RFSBLOCKCHAIN[current].Operations[i].Type == "APPEND" && RFSBLOCKCHAIN[current].Operations[i].FileName == name && startcounting {
					index++
				} else if RFSBLOCKCHAIN[current].Operations[i].Type == "CREATE" && RFSBLOCKCHAIN[current].Operations[i].FileName == name {
					rfsmutex.Unlock()
					*reply = AppendReply{index, nil}
					return nil

				}
			}
			current = RFSBLOCKCHAIN[current].PrevHash
			rfsmutex.Unlock()
		}

	} else {
		*reply = AppendReply{0, errors.New("File Does Not Exist")}
		return nil
	}

	*reply = AppendReply{0, errors.New("File Does Not Exist")}
	return nil
}

//For Miners

// Start RPC server for Miner-Miner Calls
func RPCMinerServer() {
	fmt.Println("Starting Miner RPC server")
	logger := govec.InitGoVector("server", "serverlogfile", govec.GetDefaultConfig())
	peerminer := new(PeerMinerServices)
	server := rpc.NewServer()
	server.Register(peerminer)
	localAddrBindForPeers := CONFIG.IncomingMinersAddr
	fmt.Println("Listening on ", localAddrBindForPeers, " for peer miner calls")
	l, e := net.Listen("tcp", localAddrBindForPeers)
	//defer l.Close()
	if e != nil {
		log.Fatal("listen error:", e)
	}
	options := govec.GetDefaultLogOptions()
	vrpc.ServeRPCConn(server, l, logger, options)
}

// Join takes incoming peer miner IP and adds it to list of connected miners
// Returns whole blockchain
func (m *PeerMinerServices) Join(args *JoinArgs, reply *JoinReply) error {
	fmt.Println("Received request to join from " + args.PublicAddr)

	peersMutex.Lock()
	peers[args.PublicAddr] = args.MinerID
	peersMutex.Unlock()

	_, _, longestBlockChainAsMap := getBlocksOnLongestChain()

	*reply = JoinReply{CONFIG.MinerID, longestBlockChainAsMap}

	return nil
}

func (m *PeerMinerServices) PublishOp(args *OpArgs, reply *bool) error {
	fmt.Println("Received publish op request: ", args.PublicAddr, args.Op.Id)

	opqmutex.Lock()
	//fmt.Println("OpQ locked by publishOp")

	duplicateOp := false

	// Checks whether the op is already in the queue
	for _, op := range operationsQueue {
		if op.Id == args.Op.Id {
			duplicateOp = true
			fmt.Println("Already received op with ID: " + op.Id + " Skip propagating.")
			break
		}
	}
	// Checks whether the op has already been mined
	_, longestChain, _ := getBlocksOnLongestChain()
	for _, block := range longestChain {
		if blockHasOperationId(block, args.Op.Id) {
			duplicateOp = true
			fmt.Println("Op has already been mined: " + args.Op.Id + " Skip propagating.")
			break
		} else if args.Op.Type == "CREATE" && blockHasSameCreate(block, args.Op) {
			duplicateOp = true
			fmt.Println("Create Op with name ", args.Op.FileName, " has already been mined. Skip propagating.")
			break
		}
	}
	//fmt.Println("OpQ unlocking by publishOp")
	opqmutex.Unlock()

	if !duplicateOp {
		appendToOperationsQueue(args.Op)
	}

	if !duplicateOp {
		err := propagateOp(args)
		if err == nil {
			*reply = true
		}
		return err
	}

	*reply = false
	return nil
}

func (m *PeerMinerServices) PublishBlock(args *BlockArgs, isValidBlock *bool) error {
	//fmt.Println("Received block: " + args.Hash + " From miner: " + args.PublicAddr)
	h := md5.New()
	var blockbytes []byte

	blockbytes, err := json.Marshal(args.Block)
	if err != nil {
		fmt.Println("error: ", err)
	}

	h.Write(blockbytes)
	blockHash := hex.EncodeToString(h.Sum(nil))

	valid, _, invalidtype := ValidateBlock(args.Block, true)
	if valid {
		RFSBLOCKCHAIN[blockHash] = args.Block
		//fmt.Println("Added valid block to local blockchain: " + args.Hash + " From miner: " + args.PublicAddr)
	} else {

		if invalidtype == "hash" {
			updateBeforeMining = true
			//updateLocalBlockChainFromPeer(args.LocalAddr)
			//setCreateFileConfirmBlockHash()
			//setAppendFileConfirmBlockHash()
			ret := true
			isValidBlock = &ret
			return nil
		} else if invalidtype == "create" {
			updateBeforeMining = true
			//updateLocalBlockChainFromPeer(args.LocalAddr)
			//setCreateFileConfirmBlockHash()
			//setAppendFileConfirmBlockHash()
			isValidBlock = &valid
			return nil
		} else {
			isValidBlock = &valid
			return nil
		}

	}
	//ret := true
	isValidBlock = &valid
	return nil
}

func (m *PeerMinerServices) GetBlockChain(args *AddrArgs, reply *BlockChainReply) error {

	//fmt.Println("Received request for a copy of the blockchain from: " + args.LocalAddr)

	_, _, longestChainMap := getBlocksOnLongestChain()

	*reply = BlockChainReply{longestChainMap}

	return nil
}

func appendToOperationsQueue(operation Operation) {
	/*appendToEndOfQueue := true
	opSeqNum := operation.SeqNum

	opqmutex.Lock()
	//defer opqmutex.Unlock()

	operationsQueueCopy := operationsQueue
	opqmutex.Unlock()

	for index, op := range operationsQueueCopy {

		if op.MinerID == operation.MinerID && op.SeqNum > opSeqNum {
			if index == 0 {
				opqmutex.Lock()
				operationsQueue = append([]Operation{operation}, operationsQueue...)
				opqmutex.Unlock()
			} else {
				opqmutex.Lock()
				prependOps := operationsQueue[:index-1]
				appendOps := operationsQueue[index:]
				operationsQueue = append(prependOps, operation)
				operationsQueue = append(operationsQueue, appendOps...)
				opqmutex.Unlock()
			}
			appendToEndOfQueue = false

			break
		}
	}

	if appendToEndOfQueue {
		opqmutex.Lock()
		operationsQueue = append(operationsQueue, operation)
		opqmutex.Unlock()
	}*/
	opqmutex.Lock()
	operationsQueue = append(operationsQueue, operation)
	opqmutex.Unlock()
}

func prependToOperationsQueue(operation Operation) {
	/*prependToQueue := true
	opSeqNum := operation.SeqNum

	opqmutex.Lock()
	defer opqmutex.Unlock()

	for index := len(operationsQueue) - 1; index >= 0; index-- {
		op := operationsQueue[index]
		if op.MinerID == operation.MinerID && op.SeqNum < opSeqNum {
			if index == len(operationsQueue)-1 {
				operationsQueue = append(operationsQueue, operation)
			} else {
				prependOps := operationsQueue[:index]
				appendOps := operationsQueue[index+1:]
				operationsQueue = append(prependOps, operation)
				operationsQueue = append(operationsQueue, appendOps...)
			}
			prependToQueue = false
			break
		}
	}
	if prependToQueue {
		operationsQueue = append([]Operation{operation}, operationsQueue...)
	}*/
	opqmutex.Lock()
	operationsQueue = append([]Operation{operation}, operationsQueue...)
	opqmutex.Unlock()
}

func propagateOp(args *OpArgs) error {

	for address := range getPeers() {
		if address != args.PublicAddr {
			fmt.Println("Sending op "+args.Op.Id+" to: ", address)
			peer, err := vrpc.RPCDial("tcp", address, logger, options)
			if err != nil {
				fmt.Println("Failed to send: ", address, " not accepting connections")
				removePeer(address)
				return err
			} else {
				sendargs := args
				var reply bool

				publishCall := peer.Go("PeerMinerServices.PublishOp", sendargs, reply, nil)
				replyCall := <-publishCall.Done
				if replyCall.Error != nil {
					fmt.Println("Error propagating operation to "+address, replyCall.Error)
					removePeer(address)
					if peer != nil {
						peer.Close()
					}
					return err
				}
				if peer != nil {
					peer.Close()
				}
			}
		} else {
			fmt.Println("Request flooded back to source. Skip propagating.")
			break
		}
	}
	return nil
}

func (miner *minerlib) PublishOpToPeerMiners(op Operation) (err error) {
	fmt.Println("Publishing op request to peers from: ", publicIpAddr, op.MinerID)
	printOpDetails(op)
	err = propagateOp(&OpArgs{PublicAddr: publicIpAddr, Op: op})
	return err
}

func printOpDetails(op Operation) {
	fmt.Println(op.TimeStamp.String(), op.Type, op.Id, op.MinerID, op.FileName)
}

func blockHasOperationId(block Block, opId string) bool {
	for _, op := range block.Operations {
		if op.Id == opId {
			return true
		}
	}
	return false
}

/*func blockHasOpWithMinerId(block Block, minerId string) bool {
	for _, op := range block.Operations {
		if op.MinerID == minerId {
			return true
		}
	}
	return false
}*/

func blockHasSameCreate(block Block, opToValidate Operation) bool {
	for _, op := range block.Operations {
		if op.Type == "CREATE" && opToValidate.FileName == op.FileName {
			return true
		}
	}
	return false
}

func validateMinedBlock(block Block) (bool, []Operation) {
	// Checking for No Duplicate Ops
	var duplicateOps = false
	var opsToRetry = []Operation{}
	var opsToThrowOut = []Operation{}

	lastHash, _, longestBlockChain := getBlocksOnLongestChain()
	//fmt.Println("Got longest chain for validating block")
	for _, blockOnChain := range longestBlockChain {
		for _, opOnChain := range blockOnChain.Operations {
			for _, opToVerify := range block.Operations {
				if opOnChain.Id == opToVerify.Id {
					duplicateOps = true
					opsToThrowOut = append(opsToThrowOut, opToVerify)
				}
			}
		}
	}

	for _, opToReAdd := range block.Operations {
		add := true
		for _, badOp := range opsToThrowOut {
			if opToReAdd == badOp {
				add = false
			}
		}
		if add {
			opsToRetry = append(opsToRetry, opToReAdd)
		}
	}

	if duplicateOps {
		fmt.Println("Mined Block contains duplicate Operations")
		fmt.Println("Retrying:")
		for _, retryop := range opsToRetry {

			printOpDetails(retryop)
		}

		return false, opsToRetry
	}

	// Checking for No Creates with same File Name
	var duplicateCreates = false
	opsToRetry = []Operation{}
	opsToThrowOut = []Operation{}

	for _, blockOnChain := range longestBlockChain {
		for _, opOnChain := range blockOnChain.Operations {
			for _, opToVerify := range block.Operations {
				if opToVerify.Type == "CREATE" && opOnChain.Type == "CREATE" {
					if opToVerify.FileName == opOnChain.FileName {
						duplicateCreates = true
						opsToThrowOut = append(opsToThrowOut, opToVerify)
					}
				}
			}
		}
	}

	// Retries all non duplicate creates
	for _, opToReAdd := range block.Operations {
		add := true
		for _, badOp := range opsToThrowOut {
			if opToReAdd == badOp {
				add = false
			}
		}
		if add {
			opsToRetry = append(opsToRetry, opToReAdd)
		}
	}

	if duplicateCreates {
		fmt.Println("Mined Block contains Create ops with same name")
		fmt.Println("Retrying:")
		for _, retryop := range opsToRetry {

			printOpDetails(retryop)
		}
		return false, opsToRetry
	}

	// Checking if each Op is affordable
	opsToRetry = []Operation{}

	// calculates balance for each miner in longest chain
	walletForMiners := make(map[string]int)
	current := lastHash
	for current != CONFIG.GenesisBlockHash {
		isNoOpBlock := true

		for _, op := range longestBlockChain[current].Operations {
			walletForMiners[op.MinerID] = walletForMiners[op.MinerID] - operationCost(op)
			isNoOpBlock = false
		}

		if isNoOpBlock {
			walletForMiners[longestBlockChain[current].BlockMinerID] += CONFIG.MinedCoinsPerNoOpBlock
		} else {
			walletForMiners[longestBlockChain[current].BlockMinerID] += CONFIG.MinedCoinsPerOpBlock
		}

		current = longestBlockChain[current].PrevHash
	}

	// checks each op against miner balance
	bankrupt := false
	/*for miner, coins:= range walletForMiners{
		fmt.Println("Miner ", miner, " has ", coins, " coins")
	}*/
	for _, op := range block.Operations {
		walletForMiners[op.MinerID] -= operationCost(op)
		if walletForMiners[op.MinerID] < 0 {
			bankrupt = true
		} else {
			opsToRetry = append(opsToRetry, op)
		}
	}

	if bankrupt {
		fmt.Println("Miner couldn't afford one of the ops")
		updateBeforeMining = true
		return false, block.Operations
	}

	fmt.Println("Mined block is valid")
	return true, []Operation{}

}

/**
Validates PoW
Validate coin balance and operations can be afforded by each miner
Validates each op is not already on longest chain
*/
func ValidateBlock(block Block, validateOnLongestChain bool) (bool, []Operation, string) {

	var intendedChain []Block
	var visitedHashes = make(map[string]int)
	var nextHash = block.PrevHash
	//fmt.Println("ValidateBlock waiting on rfsmutex")
	//rfsmutex.Lock()
	//fmt.Println("ValidateBlock locking on rfsmutex")
	//defer rfsmutex.Unlock()

	//Create intended chain for validating remotely mined block
	for nextHash != "" {
		if visitedHashes[nextHash] == 1 {
			fmt.Println("Block forms a loop")
			updateLocalBlockChainFromPeers()
			return false, nil, "hash"
		}

		//fmt.Println("ValidateBlock waiting on rfsmutex")
		rfsmutex.Lock()
		//fmt.Println("ValidateBlock locking on rfsmutex")

		currentBlock, ok := RFSBLOCKCHAIN[nextHash]
		rfsmutex.Unlock()
		if !ok {
			fmt.Println("Block with hash not found")
			updateBeforeMining = true
			updateLocalBlockChainFromPeers()
			return false, nil, "hash"
		}

		visitedHashes[nextHash] = 1
		intendedChain = append(intendedChain, currentBlock)

		nextHash = currentBlock.PrevHash
	}

	//Check whether operation has been committed to the chain already
	for _, intendedChainBlock := range intendedChain {
		//for every op in block to check
		for _, opToValidate := range block.Operations {
			if opToValidate.Type == "CREATE" {
				if blockHasOperationId(intendedChainBlock, opToValidate.Id) || blockHasSameCreate(intendedChainBlock, opToValidate) {
					fmt.Println("Create already exists for this name: " + opToValidate.FileName)
					return false, nil, "create"
				}
			} else if blockHasOperationId(intendedChainBlock, opToValidate.Id) {
				fmt.Println("Operation is already on the intended chain: " + opToValidate.Id)
				return false, nil, "exists"
			}
		}
	}

	//fmt.Println("ValidateBlock waiting on rfsmutex")
	rfsmutex.Lock()
	//defer rfsmutex.Unlock()
	//fmt.Println("ValidateBlock locking on rfsmutex")
	// Previous block hash points to a legal, previously generated, block
	if _, ok := RFSBLOCKCHAIN[block.PrevHash]; ok {
		rfsmutex.Unlock()
		h := md5.New()
		var blockbytes []byte

		blockbytes, err := json.Marshal(block)
		if err != nil {
			fmt.Println("error: ", err)
		}

		h.Write(blockbytes)
		str := hex.EncodeToString(h.Sum(nil))

		var zeros = ""
		var numzeros = 0
		op := len(block.Operations) > 0
		if op {
			zeros = strings.Repeat("0", CONFIG.PowPerOpBlock)
			numzeros = CONFIG.PowPerOpBlock
		} else {
			zeros = strings.Repeat("0", CONFIG.PowPerNoOpBlock)
			numzeros = CONFIG.PowPerNoOpBlock
		}

		// PoW is correct and has the right difficulty
		if str[len(str)-numzeros:] != zeros {
			return false, block.Operations, "pow"
		}

		//MinerID->Cost
		costForMiner := make(map[string]int)
		// Validate ops are valid for each miner
		for _, op := range block.Operations {
			costForMiner[op.MinerID] = costForMiner[op.MinerID] + operationCost(op)
		}

		for minerId, totalCostForMinerOps := range costForMiner {
			//GetRecordCoinBalance returns err if balance becomes invalid along the longest chain
			var minerBalance int
			if validateOnLongestChain {
				minerBalance, err = GetRecordCoinBalance(minerId, validateOnLongestChain, block.PrevHash)
				if err != nil {
					return false, block.Operations, "balance"
				}
			}

			if minerBalance-totalCostForMinerOps < 0 {
				//This block is not affordable for a miner
				return false, block.Operations, "balance"
			}
		}
		return true, nil, ""

	} else {
		rfsmutex.Unlock()
		return false, block.Operations, "hash"
	}
}

func retryOps(operations []Operation) {
	var operationsFromClient []Operation

	for _, op := range operations {
		if op.MinerID == CONFIG.MinerID {
			operationsFromClient = append(operationsFromClient, op)
		} else {
			prependToOperationsQueue(op)
		}
	}

	if len(operationsFromClient) > 0 {
		opsToDisseminateMutex.Lock()
		opsToDisseminateQueue = append(operationsFromClient, opsToDisseminateQueue...)
		opsToDisseminateMutex.Unlock()
	}
}

/*func getLastSeqNumOfMinerOnLongestChain(minerId string) int {
	result := 0
	_, longestChain, _ := getBlocksOnLongestChain()
	for _, block := range longestChain {
		if blockHasOpWithMinerId(block, minerId) {
			for _, op := range block.Operations {
				if op.MinerID == minerId && op.SeqNum > result {
					result = op.SeqNum
				}
			}
			break
		}
	}

	return result
}*/

func getNumPeers() int {
	peersMutex.Lock()
	defer peersMutex.Unlock()
	return len(peers)
}

func (miner *minerlib) GetPeerMiners() map[string]string {
	peersMutex.Lock()
	results := peers
	peersMutex.Unlock()

	return results
}

func (miner *minerlib) GetConfig() Config {
	return CONFIG
}

func (miner *minerlib) GetOpsToDisseminate() []Operation {
	opsToDisseminateMutex.Lock()
	ops := opsToDisseminateQueue
	opsToDisseminateMutex.Unlock()

	return ops
}

func (miner *minerlib) GetOpQueue() []Operation {
	opqmutex.Lock()
	ops := operationsQueue
	opqmutex.Unlock()

	return ops
}
