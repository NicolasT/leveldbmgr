{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Int (Int64)
import Data.Word (Word8)

import Control.Monad (unless)

import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as LBS

import Data.Binary (Binary, decode, encode)

import Test.Framework (Test, defaultMain, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Test.HUnit hiding (Test)

import Test.QuickCheck (NonEmptyList(..), Positive(..))

import Data.Digest.CRC32C (CRC32, crc32c, extend)

import qualified Database.LevelDB.Format.Log as Log
import qualified Database.LevelDB.Format.WriteBatch as WB

main :: IO ()
main = defaultMain tests

tests :: [Test]
tests =
    [ testGroup "CRC32C"
          [ testCase "32x 0x00" $ test_crc32 (LBS.replicate 32 0) 0x8a9136aa
          , testCase "32x 0xFF" $ test_crc32 (LBS.replicate 32 0xFF) 0x62a8ab43
          , testCase "32 asc" $ test_crc32 (LBS.pack [0 .. 31]) 0x46dd794e
          , testCase "32 desc" $ test_crc32 (LBS.pack [31, 30 .. 0]) 0x113fdb5c
          , testCase "crc32c \"a\" /= crc32c \"foo\"" $ test_crc32_ne "a" "foo"
          , testProperty "extend" prop_crc32c_extend
          ]
    , testGroup "Log"
          [ testProperty "decode/encode Record" (prop_decode_encode :: Log.Record -> Bool)
          ]
    , testGroup "WriteBatch"
          [ testProperty "decode/encode WriteBatch" (prop_decode_encode :: WB.WriteBatch -> Bool)
          ]
    ]

test_crc32 :: ByteString -> CRC32 -> Assertion
test_crc32 dat crc32 = crc32c dat @?= crc32

test_crc32_ne :: ByteString -> ByteString -> Assertion
test_crc32_ne a b = assertNotEqual "" (crc32c a) (crc32c b)
  where
    assertNotEqual preface v1 v2 =
        unless (v1 /= v2) (assertFailure msg)
      where
        msg = (if null preface then "" else preface ++ "\n") ++
              "got " ++ show v1 ++ "\n and: " ++ show v2


prop_crc32c_extend :: NonEmptyList Word8 -> Positive Int64 -> Bool
prop_crc32c_extend (NonEmpty l) (Positive a) = extend (crc32c h) t == crc32c bs
  where
    bs = LBS.pack l
    (h, t) = LBS.splitAt (fromIntegral $ a `rem` LBS.length bs) bs

prop_decode_encode :: (Eq a, Binary a) => a -> Bool
prop_decode_encode a = decode (encode a) == a
