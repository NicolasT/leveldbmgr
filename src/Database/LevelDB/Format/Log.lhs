> -- |
> -- Module     : Database.LevelDB.Format.Log
> -- Copyright  : (c) 2013, Nicolas Trangez
> -- License    : LGPL-2
> -- Maintainer : ikke@nicolast.be
> --
> -- An implementation of the LevelDB @log@ format.

Note: the plaintext parts of this file are taken from doc/log_format.txt in
the LevelDB distribution, revision 40768657bc8e.

> module Database.LevelDB.Format.Log (
>     -- * Basic blocks
>       Block(..)
>     , splitBlocks
>     -- * Records
>     , Record(..)
>     , RecordType(..)
>     , RecordData
>     , checkRecord
>     , parseBlock
>     -- * Merging records
>     , PartialRecord(..)
>     , mergeRecord
>     , mergeRecords
>     -- * Utilities
>     , validateRecords
>     , logToRecords
>     ) where
>
> import Control.Monad (unless)
> import Control.Applicative
> import Control.Exception.Base (assert)
>
> import Data.Bits
> import Data.Word
> import Data.Monoid ((<>))
>
> import Data.ByteString.Builder (Builder, lazyByteString, toLazyByteString)
> import Data.ByteString.Lazy (ByteString)
> import qualified Data.ByteString as BS
> import qualified Data.ByteString.Lazy as LBS
>
> import qualified Data.Binary as B
> import qualified Data.Binary.Get as B
> import qualified Data.Binary.Put as B
>
> import Data.Conduit
> import qualified Data.Conduit.Binary as CB
>
> import Test.QuickCheck (Arbitrary(..), choose)
>
> import Data.Digest.CRC32C (crc32c)

The log file contents are a sequence of 32KB blocks.  The only
exception is that the tail of the file may contain a partial block.

> -- | A 'Block' is the basic building unit of a @log@ file.
> newtype Block = Block { unBlock :: ByteString }
>   deriving (Show, Eq)
>
> -- | A 'Conduit' which splits a stream of 'BS.ByteString's into 'Block's.
> splitBlocks :: Monad m => Conduit BS.ByteString m Block
> splitBlocks = loop
>   where
>     loop = do
>         dat <- CB.take blockSize
>         unless (LBS.null dat) $ do
>             yield $ Block dat
>             unless (LBS.length dat < blockSize)
>                 loop
>
>     blockSize :: Num a => a
>     blockSize = 32 * 1024
>     {-# INLINE blockSize #-}

Each block consists of a sequence of records:
   block := record* trailer?
   record :=
	checksum: uint32	// crc32c of type and data[] ; little-endian
	length: uint16		// little-endian
	type: uint8		// One of FULL, FIRST, MIDDLE, LAST
	data: uint8[length]

> -- | Data contained in a 'Record'.
> type RecordData = ByteString
>
> -- | Representation of a single 'Record'.
> data Record = Record { recordChecksum :: !Word32 -- ^ CRC32C of type tag and data
>                      , recordLength :: !Word16   -- ^ Length of record data
>                      , recordTypeTag :: !Word8   -- ^ Tag of record type
>                      , recordType :: !RecordType -- ^ Record type
>                      , recordData :: !RecordData -- ^ Record data
>                      }
>   deriving (Show, Eq)
>
> instance Arbitrary Record where
>     arbitrary = do
>         typ <- arbitrary
>         dat <- LBS.pack <$> arbitrary
>
>         let tag = fromIntegral $ fromEnum typ
>             cs = crc32c $ LBS.cons tag dat
>             len = fromIntegral $ LBS.length dat
>
>         return $ Record cs len tag typ dat
>
>     shrink r = do
>         dat <- LBS.pack <$> shrink (LBS.unpack $ recordData r)
>
>         let cs = crc32c $ LBS.cons (recordTypeTag r) dat
>             len = fromIntegral $ LBS.length dat
>
>         return $ Record cs len (recordTypeTag r) (recordType r) dat
>
> instance B.Binary Record where
>     get = do
>         checksum <- unmask `fmap` B.getWord32le
>         len <- B.getWord16le
>         tag <- B.getWord8
>         let typ = toEnum $ fromIntegral tag
>         dat <- B.getLazyByteString $ fromIntegral len
>
>         return $ Record checksum len tag typ dat
>      where
>         unmask crc = let rot = crc - maskDelta in (rot `shiftR` 17) .|. (rot `shiftL` 15)
>         maskDelta = 0xA282EAD8
>
>     put r = do
>         B.putWord32le $ mask $ recordChecksum r
>         B.putWord16le $ fromIntegral $ LBS.length $ recordData r
>         -- Could use recordTypeTag instead of the recordType enum
>         B.putWord8 $ fromIntegral $ fromEnum $ recordType r
>         B.putLazyByteString $ recordData r
>       where
>         mask crc = ((crc `shiftR` 15) .|. (crc `shiftL` 17)) + maskDelta
>         maskDelta = 0xA282EAD8
>
> -- | Check a 'Record' for corruption.
> checkRecord :: Record -> Bool
> checkRecord r | recordType r == Unknown = False
>               | checksum /= recordChecksum r = False
>               | otherwise = True
>   where
>     checksum = crc32c $ LBS.cons (recordTypeTag r) (recordData r)

A record never starts within the last six bytes of a block (since it
won't fit).  Any leftover bytes here form the trailer, which must
consist entirely of zero bytes and must be skipped by readers.  

> trailer :: ByteString
> trailer = LBS.replicate 6 0
> {-# INLINE trailer #-}

Aside: if exactly seven bytes are left in the current block, and a new
non-zero length record is added, the writer must emit a FIRST record
(which contains zero bytes of user data) to fill up the trailing seven
bytes of the block and then emit all of the user data in subsequent
blocks.

More types may be added in the future.  Some Readers may skip record
types they do not understand, others may report that some data was
skipped.

FULL == 1
FIRST == 2
MIDDLE == 3
LAST == 4

> -- | Type of a 'Record'.
> data RecordType = Full
>                 | First
>                 | Middle
>                 | Last
>                 | Unknown
>   deriving (Show, Eq)
>
> instance Enum RecordType where
>     fromEnum t = case t of
>         Full -> 1
>         First -> 2
>         Middle -> 3
>         Last -> 4
>         Unknown -> error "fromEnum Unknown"
>
>     toEnum i = case i of
>         1 -> Full
>         2 -> First
>         3 -> Middle
>         4 -> Last
>         _ -> Unknown
>
> instance Bounded RecordType where
>     minBound = Full
>     maxBound = Last
>
> instance Arbitrary RecordType where
>     arbitrary = do
>         let mn = minBound
>             mx = maxBound `asTypeOf` mn
>         n <- choose (fromEnum mn, fromEnum mx)
>         return $ toEnum n `asTypeOf` mn
>
> -- | A 'Conduit' which parses a stream of 'Block's into a stream of 'Record's.
> parseBlock :: Monad m => Conduit Block m Record
> parseBlock = awaitForever $ parseRecords . unBlock
>   where
>     parseRecords dat | LBS.length dat <= 6 =
>                          assert' $ dat `LBS.isPrefixOf` trailer
>                      -- Note using runGetOrFail is safe, since all we handle here
>                      -- are Blocks, and every Record should be contained inside a
>                      -- single Block: there should at all times be sufficient input
>                      | otherwise = case B.runGetOrFail B.get dat of
>                          Right (leftOver, cnt, rec) -> do
>                              assert' $ cnt == 4 + 2 + 1 + LBS.length (recordData rec)
>                              yield rec
>                              parseRecords leftOver
>                          Left (_, _, e) -> fail $ "parseBlocks: " ++ e
>     assert' b = assert b (return ())

The FULL record contains the contents of an entire user record.

FIRST, MIDDLE, LAST are types used for user records that have been
split into multiple fragments (typically because of block boundaries).
FIRST is the type of the first fragment of a user record, LAST is the
type of the last fragment of a user record, and MID is the type of all
interior fragments of a user record.

Example: consider a sequence of user records:
   A: length 1000
   B: length 97270
   C: length 8000
A will be stored as a FULL record in the first block.

B will be split into three fragments: first fragment occupies the rest
of the first block, second fragment occupies the entirety of the
second block, and the third fragment occupies a prefix of the third
block.  This will leave six bytes free in the third block, which will
be left empty as the trailer.

C will be stored as a FULL record in the fourth block.

> -- Code below just screams for GADTs and DataKinds to get rid of all 'error' calls
>
> -- | Reconstruction state for data split across multiple 'Record's.
> data PartialRecord = Empty
>                    | Done !RecordData
>                    | Partial !Builder
>   deriving (Show)
>
> -- | Merge a 'PartialRecord' with a 'Record' coming after it.
> --
> -- Initially 'Empty' should be used as 'PartialRecord' value.
> mergeRecord :: PartialRecord -> Record -> PartialRecord
> mergeRecord mr r = case recordType r of
>     Full -> case mr of
>         Empty -> Done $ recordData r
>         _ -> error $ "mergeRecords: Full record after " ++ show mr
>     First -> case mr of
>         Empty -> Partial $ lazyByteString $ recordData r
>         _ -> error $ "mergeRecords: First record after " ++ show mr
>     Middle -> case mr of
>         Partial b -> Partial $ b <> lazyByteString (recordData r)
>         _ -> error $ "mergeRecords: Middle record after " ++ show mr
>     Last -> case mr of
>         Partial b -> Done $ toLazyByteString $ b <> lazyByteString (recordData r)
>         _ -> error $ "mergeRecords: Last record after " ++ show mr
>     Unknown -> error "mergeRecords: Unknown"
>
> -- | A 'Conduit' which combines 'Record's into actual data.
> mergeRecords :: Monad m => Conduit Record m RecordData
> mergeRecords = loop Empty
>   where
>     loop curr = await >>= \r -> case r of
>         Nothing -> return ()
>         Just rec -> case mergeRecord curr rec of
>             Done bs -> yield bs >> loop Empty
>             p@(Partial _) -> loop p
>             Empty -> fail "mergeRecords: Empty"


> -- | A 'Conduit' which checks all 'Record's using 'checkRecord' and 'fail's
> --   when a corrupted record is found.
> validateRecords :: Monad m => Conduit Record m Record
> validateRecords = awaitForever check
>   where
>     check rec = if checkRecord rec
>                     then yield rec
>                     else fail "validateRecords: invalid record"

> -- | A 'Conduit' which turns a 'Source' of 'BS.ByteString's into a 'Source'
> --   of 'Record's.
> --
> -- Basically a pipeline of 'splitBlocks', 'parseBlock', 'validateRecords' and
> -- 'mergeRecords'.
> --
> -- This can e.g. be used with 'CB.sourceFile' to parse a complete @log@ file.
> logToRecords :: Monad m => Conduit BS.ByteString m RecordData
> logToRecords = splitBlocks =$= parseBlock =$= validateRecords =$= mergeRecords

===================

Some benefits over the recordio format:

(1) We do not need any heuristics for resyncing - just go to next
block boundary and scan.  If there is a corruption, skip to the next
block.  As a side-benefit, we do not get confused when part of the
contents of one log file are embedded as a record inside another log
file.

(2) Splitting at approximate boundaries (e.g., for mapreduce) is
simple: find the next block boundary and skip records until we
hit a FULL or FIRST record.

(3) We do not need extra buffering for large records.

Some downsides compared to recordio format:

(1) No packing of tiny records.  This could be fixed by adding a new
record type, so it is a shortcoming of the current implementation,
not necessarily the format.

(2) No compression.  Again, this could be fixed by adding new record types.
