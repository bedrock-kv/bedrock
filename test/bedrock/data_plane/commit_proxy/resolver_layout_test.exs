defmodule Bedrock.DataPlane.CommitProxy.ResolverLayoutTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.ResolverLayout

  describe "from_layout/1" do
    test "returns Single struct for single resolver" do
      layout = %{resolvers: [{"", :resolver_1}]}

      assert %ResolverLayout.Single{resolver_ref: :resolver_1} = ResolverLayout.from_layout(layout)
    end

    test "returns Sharded struct for multiple resolvers" do
      layout = %{
        resolvers: [
          {"", :resolver_1},
          {"m", :resolver_2},
          {"z", :resolver_3}
        ]
      }

      result = ResolverLayout.from_layout(layout)

      assert %ResolverLayout.Sharded{
               resolver_ends: [
                 {"m", :resolver_1},
                 {"z", :resolver_2},
                 {"\xff\xff\xff", :resolver_3}
               ],
               resolver_refs: [:resolver_1, :resolver_2, :resolver_3]
             } = result
    end

    test "handles unordered resolvers correctly" do
      layout = %{
        resolvers: [
          {"z", :resolver_3},
          {"", :resolver_1},
          {"m", :resolver_2}
        ]
      }

      result = ResolverLayout.from_layout(layout)

      assert %ResolverLayout.Sharded{
               resolver_ends: [
                 {"m", :resolver_1},
                 {"z", :resolver_2},
                 {"\xff\xff\xff", :resolver_3}
               ],
               resolver_refs: [:resolver_3, :resolver_1, :resolver_2]
             } = result
    end
  end

  describe "calculate_resolver_ends/1" do
    test "calculates correct end boundaries for sorted resolvers" do
      resolvers = [
        {"", :resolver_1},
        {"m", :resolver_2}
      ]

      assert [
               {"m", :resolver_1},
               {"\xff\xff\xff", :resolver_2}
             ] = ResolverLayout.calculate_resolver_ends(resolvers)
    end

    test "sorts unsorted resolvers before calculating ends" do
      resolvers = [
        {"z", :resolver_3},
        {"", :resolver_1},
        {"m", :resolver_2}
      ]

      result = ResolverLayout.calculate_resolver_ends(resolvers)

      assert [
               {"m", :resolver_1},
               {"z", :resolver_2},
               {"\xff\xff\xff", :resolver_3}
             ] = result
    end

    test "handles single resolver" do
      resolvers = [{"", :resolver_1}]

      assert [{"\xff\xff\xff", :resolver_1}] = ResolverLayout.calculate_resolver_ends(resolvers)
    end
  end

  describe "pattern matching" do
    test "can pattern match on Single variant" do
      layout = ResolverLayout.from_layout(%{resolvers: [{"", :resolver_1}]})

      result =
        case layout do
          %ResolverLayout.Single{resolver_ref: ref} -> {:single, ref}
          %ResolverLayout.Sharded{} -> :sharded
        end

      assert {:single, :resolver_1} = result
    end

    test "can pattern match on Sharded variant" do
      layout = ResolverLayout.from_layout(%{resolvers: [{"", :r1}, {"m", :r2}]})

      result =
        case layout do
          %ResolverLayout.Single{} -> :single
          %ResolverLayout.Sharded{resolver_refs: refs} -> {:sharded, refs}
        end

      assert {:sharded, [:r1, :r2]} = result
    end
  end
end
