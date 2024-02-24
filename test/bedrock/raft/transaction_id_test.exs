defmodule Bedrock.Raft.TransactionIDTest do
  use ExUnit.Case, async: true

  alias Bedrock.Raft.TransactionID

  describe "Transaction comparisons" do
    test "tuple transaction ids order appropriately" do
      assert {2, 1} > {2, 0}
      assert {3, 0} > {2, 0}
      assert {3, 0} > {2, 1}
    end

    test "binary transaction ids order appropriately" do
      assert TransactionID.encode({2, 1}) > TransactionID.encode({2, 0})
      assert TransactionID.encode({3, 0}) > TransactionID.encode({2, 0})
      assert TransactionID.encode({3, 0}) > TransactionID.encode({2, 1})

      t1 = TransactionID.encode({1, 1})
      t2 = TransactionID.encode({127, 2})
      t3 = TransactionID.encode({128, 3})
      t4 = TransactionID.encode({16_383, 4})
      t5 = TransactionID.encode({16_384, 3})
      t6 = TransactionID.encode({16_384, 5})

      assert t2 > t1
      assert t3 > t2
      assert t4 > t3
      assert t5 > t4
      assert t6 > t5
    end
  end

  describe "TransactionID.new/2" do
    test "returns an appropriate tuple" do
      assert {2, 37} == TransactionID.new(2, 37)
    end
  end

  describe "TransactionID.term/1" do
    test "returns the term of a tuple transaction" do
      assert 2 == TransactionID.term({2, 37})
    end

    test "returns the term of a binary transaction" do
      assert 2 == TransactionID.term({2, 37} |> TransactionID.encode())
    end
  end

  describe "TransactionID.index/1" do
    test "returns the index of a tuple transaction" do
      assert 37 == TransactionID.index({2, 37})
    end

    test "returns the index of a binary transaction" do
      assert 37 == TransactionID.index({2, 37} |> TransactionID.encode())
    end
  end

  describe "TransactionID.encode/1" do
    test "returns a properly encoded value for a tuple transaction" do
      assert <<21, 0, 21, 0>> = TransactionID.encode({0, 0})
      assert <<21, 1, 21, 2>> = TransactionID.encode({0x01, 2})
      assert <<22, 1, 2, 21, 3>> = TransactionID.encode({0x0102, 3})
      assert <<23, 1, 2, 3, 21, 4>> = TransactionID.encode({0x010203, 4})
      assert <<24, 1, 2, 3, 4, 21, 5>> = TransactionID.encode({0x01020304, 5})
      assert <<25, 1, 2, 3, 4, 5, 21, 6>> = TransactionID.encode({0x0102030405, 6})
      assert <<26, 1, 2, 3, 4, 5, 6, 21, 7>> = TransactionID.encode({0x010203040506, 7})
      assert <<27, 1, 2, 3, 4, 5, 6, 7, 21, 8>> = TransactionID.encode({0x01020304050607, 8})
      assert <<28, 1, 2, 3, 4, 5, 6, 7, 8, 21, 9>> = TransactionID.encode({0x0102030405060708, 9})
    end

    test "returns a properly encoded value for a binary transaction" do
      assert <<21, 1, 21, 1>> = TransactionID.encode(<<21, 1, 21, 1>>)
      assert <<21, 127, 21, 2>> = TransactionID.encode(<<21, 127, 21, 2>>)
      assert <<21, 128, 21, 3>> = TransactionID.encode(<<21, 128, 21, 3>>)
      assert <<22, 63, 255, 21, 4>> = TransactionID.encode(<<22, 63, 255, 21, 4>>)
      assert <<22, 64, 0, 21, 5>> = TransactionID.encode(<<22, 64, 0, 21, 5>>)
    end
  end

  describe "TransactionID.decode/1" do
    test "returns a properly decoded value for a binary transaction" do
      assert {0, 0} = TransactionID.decode(<<21, 0, 21, 0>>)
      assert {0x01, 2} = TransactionID.decode(<<21, 1, 21, 2>>)
      assert {0x0102, 3} = TransactionID.decode(<<22, 1, 2, 21, 3>>)
      assert {0x010203, 4} = TransactionID.decode(<<23, 1, 2, 3, 21, 4>>)
      assert {0x01020304, 5} = TransactionID.decode(<<24, 1, 2, 3, 4, 21, 5>>)
      assert {0x0102030405, 6} = TransactionID.decode(<<25, 1, 2, 3, 4, 5, 21, 6>>)
      assert {0x010203040506, 7} = TransactionID.decode(<<26, 1, 2, 3, 4, 5, 6, 21, 7>>)
      assert {0x01020304050607, 8} = TransactionID.decode(<<27, 1, 2, 3, 4, 5, 6, 7, 21, 8>>)
      assert {0x0102030405060708, 9} = TransactionID.decode(<<28, 1, 2, 3, 4, 5, 6, 7, 8, 21, 9>>)
    end

    test "returns a properly decoded value for a tuple transaction" do
      assert {1, 1} = TransactionID.decode({1, 1})
      assert {127, 2} = TransactionID.decode({127, 2})
      assert {128, 3} = TransactionID.decode({128, 3})
      assert {16_383, 4} = TransactionID.decode({16_383, 4})
      assert {16_384, 5} = TransactionID.decode({16_384, 5})
    end
  end
end
