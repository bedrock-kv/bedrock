defmodule Bedrock.DataPlane.Materializer.Olivine.IdAllocatorTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.IdAllocator

  describe "new/2" do
    test "creates new allocator with initial values" do
      allocator = IdAllocator.new(10, [5, 3, 1])

      assert allocator.max_id == 10
      assert allocator.free_ids == [5, 3, 1]
    end
  end

  describe "allocate_id/1" do
    test "allocates from free_ids when available" do
      allocator = IdAllocator.new(10, [5, 3, 1])

      {id, updated_allocator} = IdAllocator.allocate_id(allocator)

      assert id == 5
      assert updated_allocator.free_ids == [3, 1]
      assert updated_allocator.max_id == 10
    end

    test "allocates new id when free_ids is empty" do
      allocator = IdAllocator.new(10, [])

      {id, updated_allocator} = IdAllocator.allocate_id(allocator)

      assert id == 11
      assert updated_allocator.free_ids == []
      assert updated_allocator.max_id == 11
    end

    test "allocates multiple ids from free list then generates new" do
      allocator = IdAllocator.new(10, [5, 3])

      {id1, allocator} = IdAllocator.allocate_id(allocator)
      assert id1 == 5

      {id2, allocator} = IdAllocator.allocate_id(allocator)
      assert id2 == 3

      {id3, _allocator} = IdAllocator.allocate_id(allocator)
      assert id3 == 11
    end
  end

  describe "allocate_ids/2" do
    test "returns empty list when count is 0" do
      allocator = IdAllocator.new(10, [5, 3, 1])

      {ids, updated_allocator} = IdAllocator.allocate_ids(allocator, 0)

      assert ids == []
      assert updated_allocator == allocator
    end

    test "returns empty list when count is negative" do
      allocator = IdAllocator.new(10, [5, 3, 1])

      {ids, updated_allocator} = IdAllocator.allocate_ids(allocator, -5)

      assert ids == []
      assert updated_allocator == allocator
    end

    test "allocates multiple ids in order" do
      allocator = IdAllocator.new(10, [5, 3, 1])

      {ids, updated_allocator} = IdAllocator.allocate_ids(allocator, 5)

      # Should get free ids first (5, 3, 1) then new ids (11, 12)
      assert ids == [5, 3, 1, 11, 12]
      assert updated_allocator.free_ids == []
      assert updated_allocator.max_id == 12
    end
  end

  describe "recycle_id/2" do
    test "adds id to free_ids" do
      allocator = IdAllocator.new(10, [5, 3])

      updated_allocator = IdAllocator.recycle_id(allocator, 7)

      assert updated_allocator.free_ids == [7, 5, 3]
      assert updated_allocator.max_id == 10
    end
  end

  describe "recycle_ids/2" do
    test "adds multiple ids to free_ids" do
      allocator = IdAllocator.new(10, [5, 3])

      updated_allocator = IdAllocator.recycle_ids(allocator, [7, 8, 9])

      assert updated_allocator.free_ids == [7, 8, 9, 5, 3]
      assert updated_allocator.max_id == 10
    end

    test "handles empty list" do
      allocator = IdAllocator.new(10, [5, 3])

      updated_allocator = IdAllocator.recycle_ids(allocator, [])

      assert updated_allocator.free_ids == [5, 3]
      assert updated_allocator.max_id == 10
    end
  end
end
