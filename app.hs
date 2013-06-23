{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}

import Data.Binary
import Data.Typeable
import Data.Map as M
import Data.Set as S
import Data.List as L
import Data.List.Split 
import Text.Printf
import Control.Applicative
import Control.Monad
import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (runProcess,forkProcess,initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet   
import Control.Concurrent hiding (Chan, newChan)
import Control.Concurrent.STM

type Key   = Int
type Value = String
type Store = Map Key Value
data Slice = Slice Key ProcessId 
    deriving (Show,Eq,Ord)

type RoutingInfo = (Set Slice, Set NodeId)
type SharedState = TVar RoutingInfo

emptyRoutingInfo :: RoutingInfo
emptyRoutingInfo = (S.empty, S.empty)

slices :: RoutingInfo -> Set Slice
slices = fst

peers :: RoutingInfo -> Set NodeId
peers = snd

data Query = Get ProcessId Key 
    | Set ProcessId Key Value
    deriving (Show,Eq,Typeable)

data Answer = Got (Maybe Value)
    deriving (Show,Eq,Typeable)

data Claim = Claim ProcessId Key
    deriving (Show,Eq,Typeable)

data ForwardedClaim = ForwardedClaim ProcessId Key
    deriving (Show,Eq,Typeable)

instance Binary (Query) where
    put (Get p k)   = put 'g' >> put (p,k)
    put (Set p k v) = put 's' >> put (p,k,v)
    get = do h <- get
             case h of
                'g' -> do (p,k) <- get; return (Get p k)
                's' -> do (p,k,v) <- get; return (Set p k v)

instance Binary (Answer) where
    put (Got v) = put v
    get = liftM Got get

instance Binary (Claim) where
    put (Claim p k) = put (p,k)
    get = do (p,k) <- get; return (Claim p k) 

instance Binary (ForwardedClaim) where
    put (ForwardedClaim p k)    = put (p,k)
    get = do (p,k) <- get; return (ForwardedClaim p k) 

sliceProcess :: Key -> Process ()
sliceProcess k = do
    pid <- getSelfPid 
    nsend "slice-monitor" $ Claim pid k
    localKeyValueStore

localKeyValueStore :: Process ()
localKeyValueStore = do
    go M.empty
    where go :: Store -> Process ()
          go store = do
                msg <- expect
                case msg of
                    (Get pid k)   -> (handleGet pid store k) >>  go store
                    (Set pid k v) -> (handleSet pid store k v) >>= go
        
handleGet :: ProcessId -> Store -> Key -> Process ()
handleGet pid store k = send pid (Got $ M.lookup k store)

handleSet :: ProcessId -> Store -> Key -> Value -> Process (Store)
handleSet pid store k v = return $ M.insert k v store

monitorSlices :: SharedState -> Process ()
monitorSlices state = do
    getSelfPid >>= register "slice-monitor"
    forever $ receiveWait [ match (handleSliceDied state),
                            match (handleSliceClaimed state),
                            match (handleSliceForwardedClaimed state)
                          ]

modifyState :: SharedState -> (RoutingInfo -> RoutingInfo) -> Process ()
modifyState st f = liftIO $ atomically $ modifyTVar st f

readState :: SharedState -> Process (RoutingInfo)
readState st = liftIO $ atomically $ readTVar st

handleSliceDied :: SharedState -> ProcessMonitorNotification -> Process ()   
handleSliceDied state (ProcessMonitorNotification _ pid _) = do
    modifyState state f
    where f (slices, peers) = (S.filter (\(Slice k p) -> p /= pid) slices, peers)

handleSliceClaimed :: SharedState -> Claim -> Process ()
handleSliceClaimed state (Claim pid k) = do 
    enableKeyRouting state pid k
    forwardClaimToPeers state (ForwardedClaim pid k)

handleSliceForwardedClaimed :: SharedState -> ForwardedClaim -> Process ()
handleSliceForwardedClaimed state (ForwardedClaim pid k) = enableKeyRouting state pid k

enableKeyRouting :: SharedState -> ProcessId -> Key -> Process ()
enableKeyRouting state pid k = do
    monitor pid
    modifyState state f
    where f (slices, peers) = (S.insert (Slice k pid) slices, peers)

forwardClaimToPeers :: SharedState -> ForwardedClaim -> Process ()
forwardClaimToPeers state claim  = do
    info <- readState state
    mapM_ (\n -> nsendRemote n "slice-monitor" claim) (S.toList $ peers info)

sliceForKey :: Key -> [Slice] -> Maybe Slice
sliceForKey _ []  = Nothing
sliceForKey k0 xs = Just $ head (candidates ++ dflt)
    where xs'         = sort xs
          dflt        = take 1 xs'
          candidates  = L.filter (\(Slice k1 _) -> k1 < k0) xs'

getRoute :: SharedState -> Key -> Process (Maybe Slice)
getRoute state k = liftM (sliceForKey k . S.toList . slices) (readState state)

remotable ['sliceProcess]
remotables = __remoteTable $ initRemoteTable

main = do
    args <- getArgs
    case args of
        "main":portNum:[] -> runMain portNum
        _                 -> print "usage ./app main <portnum>"

runMain :: String -> IO ()
runMain portNum = do
    -- initial setup
    b  <- initializeBackend "localhost" portNum remotables 
    n0 <- newLocalNode b
    state <- atomically $ newTVar emptyRoutingInfo
    -- run what's needed for a new node
    forkIO $ lookupNewPeers b state
    kb <- forkProcess n0  $ readKeyboard state
    rt <- forkProcess n0  $ advertiseRoutes state
    runProcess n0   $ link kb >> link rt >> monitorSlices state

advertiseRoutes :: SharedState -> Process ()
advertiseRoutes state = forever $ do
    (slices,peers) <- readState state
    forM_ (S.toList slices) $ \(Slice k pid) -> do
        let claim = ForwardedClaim pid k
        mapM_ (\n -> nsendRemote n "slice-monitor" claim) (S.toList $ peers)
    liftIO $ threadDelay 5000000

lookupNewPeers :: Backend -> SharedState -> IO ()
lookupNewPeers b state = forever $ do
    nodes <- liftM S.fromList $ findPeers b 100000
    (currentRoutes,newNodes) <- atomically $ do
        oldState <- readTVar state
        writeTVar state (slices oldState, nodes)
        return $ (slices oldState, S.difference nodes (peers oldState))
    threadDelay 5000000
    return ()

readKeyboard :: SharedState -> Process ()
readKeyboard state = forever $ do
    args <- liftIO $ liftM (splitOn " ") getLine
    case args of
        "get":key:[]        ->  execGetKey state (read key)
        "set":key:value:[]  ->  execSetKey state (read key) value
        "claim":key:[]      ->  execClaimSlice (read key)
        "kill":key:[]       ->  execKillSlice state (read key)
        "dump":[]           ->  dumpState state
        "die":reason:[]     ->  fail reason
        _                   ->  liftIO $ putStrLn "not understood"
    return ()
    
execGetKey :: SharedState -> Key -> Process ()
execGetKey state k = do
    route <- getRoute state k
    maybe (return $ "no route for key " ++ show k) (go) route >>= liftIO . putStrLn
    where go (Slice _ pid) = do 
            me <- getSelfPid
            send pid $ Get me k
            answer <- expectTimeout 1000000
            case answer of
                Nothing                 -> return "<timeout>"
                Just (Got Nothing)      -> return "<empty>"
                Just (Got (Just val))   -> return val

execSetKey :: SharedState -> Key -> Value -> Process ()
execSetKey state k v = do
    route <- getRoute state k
    maybe (return $ "no route for key " ++ show k) (go) route >>= liftIO . putStrLn
    where go (Slice _ pid) = do 
            me <- getSelfPid
            send pid $ Set me k v
            return "sent"

execKillSlice :: SharedState -> Key -> Process ()
execKillSlice state k = do
    route <- getRoute state k
    maybe (return $ "no route for key " ++ show k) (go) route >>= liftIO . putStrLn
    where go (Slice _ pid) = kill pid "user wanted so" >> return "killed"

dumpState :: SharedState -> Process ()
dumpState st = liftIO $ ((atomically $ readTVar st) >>= print)

execClaimSlice :: Key -> Process ()
execClaimSlice key = spawnLocal (sliceProcess key) >>= liftIO . print
