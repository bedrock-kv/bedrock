defmodule Bedrock.DataPlane.CommitProxy.LayoutOptimizationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.LayoutOptimization

  describe "precompute_from_layout/1" do
    test "precomputes layout with single resolver" do
      layout = %{resolvers: [{"", :resolver_1}]}

      assert %{
               resolver_ends: [{"\xff\xff\xff", :resolver_1}],
               resolver_refs: [:resolver_1]
             } = LayoutOptimization.precompute_from_layout(layout)
    end

    test "precomputes resolver layout with multiple resolvers" do
      # Test with multiple resolvers to ensure boundary calculation works correctly
      layout = %{
        resolvers: [
          {"", :resolver_1},
          {"m", :resolver_2},
          {"z", :resolver_3}
        ]
      }

      result = LayoutOptimization.precompute_from_layout(layout)

      # First resolver ends at "m" (start of next)
      # Second resolver ends at "z" (start of next)
      # Third resolver ends at max_key
      assert %{
               resolver_ends: [
                 {"m", :resolver_1},
                 {"z", :resolver_2},
                 {"\xff\xff\xff", :resolver_3}
               ],
               resolver_refs: [:resolver_1, :resolver_2, :resolver_3]
             } = result
    end

    test "handles unordered resolvers correctly" do
      # Resolvers can be provided in any order
      layout = %{
        resolvers: [
          {"z", :resolver_3},
          {"", :resolver_1},
          {"m", :resolver_2}
        ]
      }

      result = LayoutOptimization.precompute_from_layout(layout)

      # Should be sorted by start key
      assert %{
               resolver_ends: [
                 {"m", :resolver_1},
                 {"z", :resolver_2},
                 {"\xff\xff\xff", :resolver_3}
               ],
               # Refs maintain original order from input
               resolver_refs: [:resolver_3, :resolver_1, :resolver_2]
             } = result
    end
  end
end
