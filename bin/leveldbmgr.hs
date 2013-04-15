-- |
-- Module     : Main
-- Copyright  : (c) 2013, Nicolas Trangez
-- License    : GPL-2
-- Maintainer : ikke@nicolast.be
--
-- CLI access to LevelDB

{-# LANGUAGE Arrows, GeneralizedNewtypeDeriving #-}

module Main (main) where

import Prelude hiding (log)

import Control.Exception.Base (SomeException, try)

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Reader.Class (MonadReader(ask))
import Control.Monad.Trans.Class (MonadTrans(lift))
import Control.Monad.Trans.Reader (ReaderT, runReaderT)
import Control.Monad.Trans.Resource (MonadResource, runResourceT)

import Data.Bits (shift)
import Data.List (intercalate)
import Data.Version (versionBranch)

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import System.IO
import System.Exit
import System.Environment

import Options.Applicative
import Options.Applicative.Arrows hiding (loop)

import Database.LevelDB (Options(..))
import qualified Database.LevelDB as LDB

import qualified Paths_leveldbmgr

-- * Command line parsing

data Args = Args CommonOpts Command

-- | Parser for CLI arguments.
parser :: Parser Args
parser = runA $ proc () -> do
    com <- asA commonOpts -< ()
    cmd <- (asA . subparser)
            (command "version"
                (info (pure Version)
                    (progDesc "Display version information"))
          <> command "create"
                (info createParser
                    (progDesc "Create a database"))
          <> command "get"
                (info getParser
                    (progDesc "Retrieve a key from the database"))
          <> command "range"
                (info rangeParser
                    (progDesc "Retrieve a range of key-value pairs from the database"))
          <> command "set"
                (info setParser
                    (progDesc "Set a key in the database"))
          <> command "delete"
                (info deleteParser
                    (progDesc "Delete a key from the database"))
          <> command "repair"
                (info repairParser
                    (progDesc "Repair a database"))) -< ()
    A helper -< Args com cmd


-- ** Generic options

data CommonOpts = CommonOpts { optVerbose :: Bool
                             , optPath :: String
                             }
  deriving Show

-- | Parser for generic options.
commonOpts :: Parser CommonOpts
commonOpts = CommonOpts
    <$> switch
            (  short 'v'
            <> long "verbose"
            <> help "Output logging to stderr")
    <*> strOption
            (  short 'p'
            <> long "path"
            <> value "."
            <> showDefault
            <> help "Path of the database"
            <> metavar "PATH")


-- | Parser for database open options.
openOpts :: Parser LDB.Options
openOpts = LDB.Options
    <$> option
            (  long "block-restart-interval"
            <> help "Number of keys between restart points for delta encoding of keys"
            <> value 16
            <> showDefault
            <> metavar "INT"
            <> hidden)
    <*> option
            (  long "block-size"
            <> help "Approximate size of user data packed per block"
            <> value 4096
            <> showDefault
            <> metavar "INT"
            <> hidden)
    <*> option
            (  long "cache-size"
            <> help "Internal cache size (defaults to 8MB when 0)"
            <> value 0
            <> showDefault
            <> metavar "INT"
            <> hidden)
    <*> pure Nothing
    <*> nullOption
            (  long "compression"
            <> help "Compress blocks using the specified compression algorithm ('snappy' or 'none')"
            <> value LDB.Snappy
            <> showDefault
            <> showDefaultWith showCompression
            <> reader readCompression
            <> hidden)
    <*> pure False
    <*> pure False
    <*> option
            (  long "max-open-files"
            <> help "Number of open files that can be used by the DB"
            <> value 1000
            <> showDefault
            <> metavar "INT"
            <> hidden)
    <*> switch
            (  long "paranoid-checks"
            <> help "Perform aggressive checking of the data being processed (dangerous)"
            <> internal)
    <*> option
            (  long "write-buffer-size"
            <> help "Amount of data to build up in memory before converting to a sorted on-disk file"
            <> value (4 `shift` 20)
            <> showDefault
            <> metavar "INT"
            <> hidden)
    <*> pure Nothing
  where
    showCompression v = case v of
        LDB.NoCompression -> "none"
        LDB.Snappy -> "snappy"
    readCompression v | v == "none" = Right LDB.NoCompression
                      | v == "snappy" = Right LDB.Snappy
                      | otherwise = Left $ ErrorMsg $ "Unknown compression scheme `" ++ v ++ "'"

-- | Parser for database read options.
readOpts :: Parser LDB.ReadOptions
readOpts = LDB.ReadOptions
    <$> switch
            (  long "verify-checksums"
            <> help "Verify all data read from underlying storage against corresponding checksums")
    <*> pure True
    <*> pure Nothing

-- | Parser for database write options.
writeOpts :: Parser LDB.WriteOptions
writeOpts = LDB.WriteOptions
    <$> switch
            (  long "sync"
            <> help "Force sync of OS buffers")


-- ** Specific commands

data Command = Version
             | Create LDB.Options
             | Get GetOpts LDB.Options LDB.ReadOptions
             | Set SetOpts LDB.Options LDB.WriteOptions
             | Delete DeleteOpts LDB.Options LDB.WriteOptions
             | Repair LDB.Options
             | Range RangeOpts LDB.Options LDB.ReadOptions


-- *** @create@ command

-- | Parser for @create@ options.
createParser :: Parser Command
createParser = Create <$> openOpts <**> helper


-- *** @get@ command

data GetOpts = GetOpts { getKey :: Maybe ByteString
                       , getNoLn :: Bool
                       }
  deriving Show

-- | Parser for @get@ options.
getParser :: Parser Command
getParser = Get <$> getOpts <*> openOpts <*> readOpts <**> helper
  where
    getOpts = flip GetOpts
        <$> switch
                (  short 'n'
                <> help "Don't print a newline after the value" )
        <*> (optional . argument toBS)
                (  help "Key to retrieve"
                <> metavar "KEY")


-- *** @set@ command

data SetOpts = SetOpts ByteString (Maybe ByteString)
  deriving (Show)

-- | Parser for @set@ options.
setParser :: Parser Command
setParser = Set <$> setOpts <*> openOpts <*> writeOpts <**> helper
  where
    setOpts = SetOpts
        <$> argument toBS
                (  help "Key to set"
                <> metavar "KEY")
        <*> (optional . argument toBS)
                (  help "Value to set"
                <> metavar "VALUE")


-- *** @delete@ commmand

data DeleteOpts = DeleteOpts (Maybe ByteString)
  deriving (Show)

-- | Parser for @delete@ options.
deleteParser :: Parser Command
deleteParser = Delete <$> deleteOpts <*> openOpts <*> writeOpts <**> helper
  where
    deleteOpts = DeleteOpts
        <$> (optional . argument toBS)
                (  help "Key to delete"
                <> metavar "KEY")


-- *** @repair@ command
repairParser :: Parser Command
repairParser = Repair <$> openOpts <**> helper


-- *** @range@ command

data RangeOpts = RangeOpts { rangeKeyValueDelimiter :: ByteString
                           , rangePairDelimiter :: ByteString
                           , rangeFrom :: Maybe ByteString
                           , rangeExcludeFrom :: Bool
                           , rangeTo :: Maybe ByteString
                           , rangeExcludeTo :: Bool
                           }
  deriving Show

-- | Parser for @range@ options.
rangeParser :: Parser Command
rangeParser = Range <$> rangeOpts <*> openOpts <*> readOpts <**> helper
  where
    rangeOpts = RangeOpts
        <$> nullOption
                (  long "key-value-delimiter"
                <> help "Delimiter between keys and values"
                <> value (BS8.pack " -> ")
                <> showDefault
                <> reader toBS)
        <*> nullOption
                (  long "pair-delimiter"
                <> help "Delimiter between key-value pairs"
                <> value (BS8.singleton '\n')
                <> showDefault
                <> reader toBS)
        <*> (optional . nullOption)
                (  short 'f'
                <> long "from"
                <> help "Start key"
                <> metavar "KEY"
                <> reader toBS)
        <*> switch
                (  long "exclude-from"
                <> help "Exclude the 'from' key")
        <*> (optional . nullOption)
                (  short 't'
                <> long "to"
                <> help "End key"
                <> metavar "KEY"
                <> reader toBS)
        <*> switch
                (  long "exclude-to"
                <> help "Exclude the 'to' key" )

-- ** Utilities

-- | Pack a 'String' into a 'ByteString' inside some 'Monad' @m@
toBS :: Monad m => String -> m ByteString
toBS = return . BS8.pack


-- * Command implementations

-- | Environment in which actions do their thing.
newtype Action m a = Action { unAction :: ReaderT CommonOpts m a }
  deriving (Functor, Applicative, Monad, MonadTrans, MonadIO, MonadReader CommonOpts)

-- ** Utilities

-- | Retrieve 'CommonOpts' settings from the environment.
getOptions :: Monad m => Action m CommonOpts
getOptions = ask

-- | Output a message to 'stderr' if verbose mode is enabled.
log :: MonadIO m => String -> Action m ()
log msg = do
    cfg <- getOptions
    when (optVerbose cfg) $
        liftIO $ hPutStrLn stderr $ "[LOG] " ++ msg

-- | Open a 'LDB.DB' with given 'LDB.Option's and run given action on it.
withDB :: MonadResource m => Options -> (LDB.DB -> Action m a) -> Action m a
withDB opts f = do
    cfg <- getOptions
    log $ "Opening database `" ++ optPath cfg ++ "'"
    db <- lift $ LDB.open (optPath cfg) opts
    log "Database opened"
    res <- f db
    log "Database action finished"
    return res

-- | Extract a 'ByteString' from a given 'Maybe' value, or read all of
-- 'stdin' if 'Nothing'.
maybeStdin :: MonadIO m => Maybe ByteString -> m ByteString
maybeStdin m = case m of
    Just v -> return v
    Nothing -> liftIO BS.getContents


-- ** Implementations

-- | Interpret given CLI arguments, and execute the requested command.
run :: MonadResource m => Args -> m ExitCode
run (Args opts cmd) = runReaderT (unAction act) opts
  where
    act = case cmd of
        Version -> runVersion
        Get a d r -> runGet a d r
        Set a d w -> runSet a d w
        Create d -> runCreate d
        Delete a d w -> runDelete a d w
        Repair d -> runRepair d
        Range a d r -> runRange a d r

-- | Execute the @version@ command.
runVersion :: MonadResource m => Action m ExitCode
runVersion = do
    (ma, mi) <- lift LDB.version
    liftIO $ do
        putStrLn $ "leveldbmgr version " ++ version
        putStrLn $ "libleveldb version " ++ show ma ++ "." ++ show mi
    return ExitSuccess
  where
    version = intercalate "." $ map show $ versionBranch Paths_leveldbmgr.version

-- | Execute a @get@ command.
runGet :: MonadResource m => GetOpts -> LDB.Options -> LDB.ReadOptions -> Action m ExitCode
runGet opts dbOptions readOptions = run' =<< maybeStdin (getKey opts)
  where
    run' key = withDB dbOptions $ \db -> do
        v <- lift $ LDB.get db readOptions key
        case v of
            Just v' -> do
                let put = if getNoLn opts then BS.putStr else BS8.putStrLn
                liftIO $ put v'
                return ExitSuccess
            Nothing -> do
                log "Key not found"
                return $ ExitFailure 10

-- | Execute a @set@ command.
runSet :: MonadResource m => SetOpts -> LDB.Options -> LDB.WriteOptions -> Action m ExitCode
runSet (SetOpts key v) dbOptions writeOptions = run' =<< maybeStdin v
  where
    run' val = withDB dbOptions $ \db -> do
        lift $ LDB.put db writeOptions key val
        return ExitSuccess

-- | Execute a @create@ command.
runCreate :: MonadResource m => LDB.Options -> Action m ExitCode
runCreate dbOptions = do
    opts <- getOptions
    _db <- lift $ LDB.open (optPath opts) cfg
    return ExitSuccess
  where
    cfg = dbOptions { createIfMissing = True
                    , errorIfExists = True
                    }

-- | Execute a @delete@ command.
runDelete :: MonadResource m => DeleteOpts -> LDB.Options -> LDB.WriteOptions -> Action m ExitCode
runDelete (DeleteOpts k) dbOptions writeOptions = run' =<< maybeStdin k
  where
    run' key = withDB dbOptions $ \db -> do
        lift $ LDB.delete db writeOptions key
        return ExitSuccess

-- | Execute a @repair@ command.
runRepair :: MonadResource m => LDB.Options -> Action m ExitCode
runRepair dbOptions = do
    cfg <- getOptions
    lift $ LDB.repair (optPath cfg) dbOptions
    return ExitSuccess

-- | Execute a @range@ command.
runRange :: MonadResource m => RangeOpts -> LDB.Options -> LDB.ReadOptions -> Action m ExitCode
runRange opts dbOptions readOptions = withDB dbOptions $ \db -> lift $ do
    iter <- LDB.iterOpen db readOptions
    case rangeFrom opts of
        Nothing -> LDB.iterFirst iter
        Just k -> LDB.iterSeek iter k

    valid <- LDB.iterValid iter

    if not valid
        then return $ ExitFailure 11
        else do
            Just key <- LDB.iterKey iter
            when (rangeExcludeFrom opts && (Just key == rangeFrom opts)) $
                LDB.iterNext iter

            let done = case rangeTo opts of
                    Nothing -> const False
                    Just k -> \k' -> if rangeExcludeTo opts
                                         then k' >= k
                                         else k' > k

            loop done iter
  where
    loop done iter = do
        valid <- LDB.iterValid iter
        if not valid
            then return ExitSuccess
            else do
                Just k <- LDB.iterKey iter
                if done k
                    then return ExitSuccess
                    else do
                        Just v <- LDB.iterValue iter

                        liftIO $ do
                            BS.putStr k
                            BS.putStr $ rangeKeyValueDelimiter opts
                            BS.putStr v
                            BS.putStr $ rangePairDelimiter opts

                        LDB.iterNext iter

                        loop done iter


-- * Main

-- | Main entry point.
main :: IO ()
main = do
    progName <- getProgName
    res <- execParser (opts progName) >>= try . runResourceT . run
    exitWith =<< case res of
        Left e -> do
            hPutStrLn stderr $ "Error: " ++ show (e :: SomeException)
            return $ ExitFailure 1
        Right r -> return r
  where
    opts progName = info parser
                    (fullDesc
                    <> header (progName ++ " - CLI access to LevelDB"))
