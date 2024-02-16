defmodule Bedrock.Raft.TransactionTest do
  use ExUnit.Case, async: true

  alias Bedrock.Raft.Transaction

  describe "Transaction comparisons" do
    test "tuple transactions order appropriately" do
      assert {2, 1} > {2, 0}
      assert {3, 0} > {2, 0}
      assert {3, 0} > {2, 1}
    end

    test "binary transactions order appropriately" do
      assert Transaction.encode({2, 1}) > Transaction.encode({2, 0})
      assert Transaction.encode({3, 0}) > Transaction.encode({2, 0})
      assert Transaction.encode({3, 0}) > Transaction.encode({2, 1})

      t1 = Transaction.encode({1, 1})
      t2 = Transaction.encode({127, 2})
      t3 = Transaction.encode({128, 3})
      t4 = Transaction.encode({16_383, 4})
      t5 = Transaction.encode({16_384, 5})

      assert t2 > t1
      assert t3 > t2
      assert t4 > t3
      assert t5 > t4
    end
  end

  describe "Transaction.new/2" do
    test "returns an appropriate tuple" do
      assert {2, 37} == Transaction.new(2, 37)
    end
  end

  describe "Transaction.term/1" do
    test "returns the term of a tuple transaction" do
      assert 2 == Transaction.term({2, 37})
    end

    test "returns the term of a binary transaction" do
      assert 2 == Transaction.term({2, 37} |> Transaction.encode())
    end
  end

  describe "Transaction.index/1" do
    test "returns the index of a tuple transaction" do
      assert 37 == Transaction.index({2, 37})
    end

    test "returns the index of a binary transaction" do
      assert 37 == Transaction.index({2, 37} |> Transaction.encode())
    end
  end

  describe "Transaction.encode/1" do
    test "returns a properly encoded value for a tuple transaction" do
      assert <<21, 0, 21, 0>> = Transaction.encode({0, 0})
      assert <<21, 1, 21, 2>> = Transaction.encode({0x01, 2})
      assert <<22, 1, 2, 21, 3>> = Transaction.encode({0x0102, 3})
      assert <<23, 1, 2, 3, 21, 4>> = Transaction.encode({0x010203, 4})
      assert <<24, 1, 2, 3, 4, 21, 5>> = Transaction.encode({0x01020304, 5})
      assert <<25, 1, 2, 3, 4, 5, 21, 6>> = Transaction.encode({0x0102030405, 6})
      assert <<26, 1, 2, 3, 4, 5, 6, 21, 7>> = Transaction.encode({0x010203040506, 7})
      assert <<27, 1, 2, 3, 4, 5, 6, 7, 21, 8>> = Transaction.encode({0x01020304050607, 8})
      assert <<28, 1, 2, 3, 4, 5, 6, 7, 8, 21, 9>> = Transaction.encode({0x0102030405060708, 9})
    end

    test "returns a properly encoded value for a binary transaction" do
      assert <<21, 1, 21, 1>> = Transaction.encode(<<21, 1, 21, 1>>)
      assert <<21, 127, 21, 2>> = Transaction.encode(<<21, 127, 21, 2>>)
      assert <<21, 128, 21, 3>> = Transaction.encode(<<21, 128, 21, 3>>)
      assert <<22, 63, 255, 21, 4>> = Transaction.encode(<<22, 63, 255, 21, 4>>)
      assert <<22, 64, 0, 21, 5>> = Transaction.encode(<<22, 64, 0, 21, 5>>)
    end
  end

  describe "Transaction.decode/1" do
    test "returns a properly decoded value for a binary transaction" do
      assert {0, 0} = Transaction.decode(<<21, 0, 21, 0>>)
      assert {0x01, 2} = Transaction.decode(<<21, 1, 21, 2>>)
      assert {0x0102, 3} = Transaction.decode(<<22, 1, 2, 21, 3>>)
      assert {0x010203, 4} = Transaction.decode(<<23, 1, 2, 3, 21, 4>>)
      assert {0x01020304, 5} = Transaction.decode(<<24, 1, 2, 3, 4, 21, 5>>)
      assert {0x0102030405, 6} = Transaction.decode(<<25, 1, 2, 3, 4, 5, 21, 6>>)
      assert {0x010203040506, 7} = Transaction.decode(<<26, 1, 2, 3, 4, 5, 6, 21, 7>>)
      assert {0x01020304050607, 8} = Transaction.decode(<<27, 1, 2, 3, 4, 5, 6, 7, 21, 8>>)
      assert {0x0102030405060708, 9} = Transaction.decode(<<28, 1, 2, 3, 4, 5, 6, 7, 8, 21, 9>>)
    end

    test "returns a properly decoded value for a tuple transaction" do
      assert {1, 1} = Transaction.decode({1, 1})
      assert {127, 2} = Transaction.decode({127, 2})
      assert {128, 3} = Transaction.decode({128, 3})
      assert {16_383, 4} = Transaction.decode({16_383, 4})
      assert {16_384, 5} = Transaction.decode({16_384, 5})
    end
  end
end
