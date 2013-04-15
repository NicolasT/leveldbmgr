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

import Data.List (intercalate)

import Data.Version (versionBranch)

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import System.IO
import System.Exit
import System.Environment

import Data.Default

import Options.Applicative
import Options.Applicative.Arrows

import Database.LevelDB (Options(..))
import qualified Database.LevelDB as LDB

import qualified Paths_leveldbmgr

-- * Command line parsing

data Args = Args CommonOpts Command
  deriving Show

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
          <> command "set"
                (info setParser
                    (progDesc "Set a key in the database"))
          <> command "delete"
                (info deleteParser
                    (progDesc "Delete a key from the database"))) -< ()
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


-- ** Specific commands

data Command = Version
             | Create
             | Get GetOpts
             | Set SetOpts
             | Delete DeleteOpts
  deriving Show

-- *** @create@ command

-- | Parser for @create@ options.
createParser :: Parser Command
createParser = pure Create <$> helper


-- *** @get@ command

data GetOpts = GetOpts { getKey :: Maybe ByteString
                       , getNoLn :: Bool
                       }
  deriving Show

-- | Parser for @get@ options.
getParser :: Parser Command
getParser = Get <$> getOpts <**> helper
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
setParser = Set <$> setOpts <**> helper
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
deleteParser = Delete <$> deleteOpts <**> helper
  where
    deleteOpts = DeleteOpts
        <$> (optional . argument toBS)
                (  help "Key to delete"
                <> metavar "KEY")


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
        Get a -> runGet a
        Set a -> runSet a
        Create -> runCreate
        Delete a -> runDelete a

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
runGet :: MonadResource m => GetOpts -> Action m ExitCode
runGet opts = run' =<< maybeStdin (getKey opts)
  where
    run' key = withDB def $ \db -> do
        v <- lift $ LDB.get db def key
        case v of
            Just v' -> do
                let put = if getNoLn opts then BS.putStr else BS8.putStrLn
                liftIO $ put v'
                return ExitSuccess
            Nothing -> do
                log "Key not found"
                return $ ExitFailure 10

-- | Execute a @set@ command.
runSet :: MonadResource m => SetOpts -> Action m ExitCode
runSet (SetOpts key v) = run' =<< maybeStdin v
  where
    run' val = withDB def $ \db -> do
        lift $ LDB.put db def key val
        return ExitSuccess

-- | Execute a @create@ command.
runCreate :: MonadResource m => Action m ExitCode
runCreate = do
    opts <- getOptions
    _db <- lift $ LDB.open (optPath opts) cfg
    return ExitSuccess
  where
    cfg = def { createIfMissing = True
              , errorIfExists = True
              }

-- | Execute a @delete@ command.
runDelete :: MonadResource m => DeleteOpts -> Action m ExitCode
runDelete (DeleteOpts k) = run' =<< maybeStdin k
  where
    run' key = withDB def $ \db -> do
        lift $ LDB.delete db def key
        return ExitSuccess


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
