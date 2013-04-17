-- |
-- Module     : Database.LevelDB.Format.WriteBatch
-- Copyright  : (c) 2013, Nicolas Trangez
-- License    : LGPL-2
-- Maintainer : ikke@nicolast.be
--
-- Representation of the LevelDB @WriteBatch@ type.

module Database.LevelDB.Format.WriteBatch (
      WriteBatch(..)
    , Record(..)
    ) where

import Control.Monad (replicateM)
import Control.Applicative

import Data.Bits hiding (shift)
import Data.Word

import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as LBS

import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B

import Test.QuickCheck (Arbitrary(..), oneof)

-- From db/write_batch.cc:
-- WriteBatch::rep_ :=
--    sequence: fixed64
--    count: fixed32
--    data: record[count]

-- | Representation of a 'WriteBatch'
data WriteBatch = WriteBatch { wbSequence :: !Word64  -- ^ Sequence number
                             , wbRecords :: ![Record] -- ^ Payload
                             }
  deriving (Show, Eq)

instance Arbitrary WriteBatch where
    arbitrary = WriteBatch <$> arbitrary <*> arbitrary
    shrink (WriteBatch s r) = WriteBatch <$> shrink s <*> shrink r

instance B.Binary WriteBatch where
    get = WriteBatch <$> B.getWord64le <*> getRecords
      where
        getRecords = B.getWord32le >>= flip replicateM B.get . fromIntegral

    put (WriteBatch s rs) = do
        B.putWord64le s
        B.putWord32le (fromIntegral $ length rs)
        mapM_ B.put rs


-- record :=
--    kTypeValue varstring varstring         |
--    kTypeDeletion varstring

-- | Representation of a single 'Record' contained inside a 'WriteBatch'
data Record = Value !ByteString !ByteString
            | Deletion !ByteString
  deriving (Show, Eq)

instance Arbitrary Record where
    arbitrary = oneof [ Value <$> arbitraryLBS <*> arbitraryLBS
                      , Deletion <$> arbitraryLBS
                      ]
      where
        arbitraryLBS = LBS.pack <$> arbitrary

    shrink r = case r of
        Value k v -> Value <$> shrinkLBS k <*> shrinkLBS v
        Deletion k -> Deletion <$> shrinkLBS k
      where
        shrinkLBS bs = LBS.pack <$> shrink (LBS.unpack bs)

instance B.Binary Record where
    get = B.getWord8 >>= \tag -> case tag of
            1 -> Value <$> (unVarString <$> B.get) <*> (unVarString <$> B.get)
            0 -> Deletion <$> unVarString <$> B.get
            i -> fail $ "Record: invalid tag " ++ show i

    put r = case r of
                Value k v -> B.putWord8 1 >> B.put (VarString k) >> B.put (VarString v)
                Deletion k -> B.putWord8 0 >> B.put (VarString k)


-- varstring :=
--    len: varint32
--    data: uint8[len]
newtype VarString = VarString { unVarString :: ByteString }

instance B.Binary VarString where
    get = VarString <$> (getVarWord32 >>= B.getLazyByteString . fromIntegral)
    put (VarString bs) = putVarWord32 (fromIntegral $ LBS.length bs) >> B.putLazyByteString bs


getVarWord32 :: B.Get Word32
getVarWord32 = loop 0 0
  where
    loop shift acc | shift > 28 = fail "getVarWord32: shift overflow"
                   | otherwise = loop' shift acc
    loop' shift acc = do
        r <- fromIntegral `fmap` B.getWord8
        if r .&. 128 == 0
            then return $ acc .|. (r `shiftL` shift)
            else loop (shift + 7) (acc .|. ((r .&. 127) `shiftL` shift))
    {-# INLINE loop' #-}

putVarWord32 :: Word32 -> B.Put
putVarWord32 i | i < (1 `shiftL` 7) =
                   B.putWord8 $ mask i
               | i < (1 `shiftL` 14) = do
                   B.putWord8 $ mask $ i .|. b
                   B.putWord8 $ mask $ i `shiftR` 7
               | i < (1 `shiftL` 21) = do
                   B.putWord8 $ mask $ i .|. b
                   B.putWord8 $ mask $ (i `shiftR` 7) .|. b
                   B.putWord8 $ mask $ i `shiftR` 14
               | i < (1 `shiftL` 28) = do
                   B.putWord8 $ mask $ i .|. b
                   B.putWord8 $ mask $ (i `shiftR` 7) .|. b
                   B.putWord8 $ mask $ (i `shiftR` 14) .|. b
                   B.putWord8 $ mask $ i `shiftR` 21
               | otherwise = do
                   B.putWord8 $ mask $ i .|. b
                   B.putWord8 $ mask $ (i `shiftR` 7) .|. b
                   B.putWord8 $ mask $ (i `shiftR` 14) .|. b
                   B.putWord8 $ mask $ (i `shiftR` 21) .|. b
                   B.putWord8 $ mask $ i `shiftR` 28
  where
    b = 128
    {-# INLINE b #-}
    mask = fromIntegral . (.&.) 0xFF
    {-# INLINE mask #-}
