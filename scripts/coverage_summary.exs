#!/usr/bin/env elixir

defmodule CoverageSummary do
  def decode_json(json_string) do
    try do
      # Returns the decoded data if successful
      decoded = :json.decode(json_string)
      {:ok, decoded}
    rescue
      # OTP 26's JSON parser raises ArgumentError for invalid JSON
      e in ArgumentError ->
        {:error, e.message}
    end
  end

  def main([json_path, module_name]) do
    with {:ok, json_content} <- File.read(json_path),
         {:ok, parsed_json} <- decode_json(json_content) do
      # Sort modules by coverage percentage (lowest first)
      sorted_modules = sort_modules_by_coverage(parsed_json)

      # Find the module by name
      case find_module_by_name(sorted_modules, module_name) do
        nil ->
          IO.puts("Module not found: #{module_name}")

        module ->
          # Print summary for the specific module
          print_module_summary(module)
          # Print detailed coverage for the module
          print_detailed_module_coverage(module)
      end
    else
      error -> IO.puts("Error: #{inspect(error)}")
    end
  end

  def main([json_path]) do
    with {:ok, json_content} <- File.read(json_path),
         {:ok, parsed_json} <- decode_json(json_content) do
      # Sort modules by coverage percentage (lowest first)
      sorted_modules = sort_modules_by_coverage(parsed_json)

      # Print summary table of all modules
      print_coverage_summary_table(sorted_modules)

      # Ask if user wants to see details for a specific module
      IO.puts(
        "\nEnter module number to see detailed coverage, or press Enter to see all uncovered lines:"
      )

      case IO.gets("") |> String.trim() do
        "" ->
          print_all_uncovered_lines(sorted_modules)

        num ->
          case Integer.parse(num) do
            {idx, _} when idx >= 0 and idx < length(sorted_modules) ->
              module = Enum.at(sorted_modules, idx)
              print_detailed_module_coverage(module)

            _ ->
              IO.puts("Invalid module number")
          end
      end
    else
      error -> IO.puts("Error: #{inspect(error)}")
    end
  end

  def main(_) do
    IO.puts("Usage: coverage_summary.exs path/to/excoveralls.json [module_name]")
    IO.puts("  - If module_name is provided, shows detailed coverage for that module")

    IO.puts(
      "  - module_name should be the relative path within the project (e.g., lib/collx/recommendations/popular_items.ex)"
    )
  end

  defp find_module_by_name(modules, module_name) do
    # The module name in the coverage data might have a different prefix
    # than what the user provides, so we'll do a partial match on the end of the path
    Enum.find(modules, fn %{"name" => name} ->
      String.ends_with?(name, module_name)
    end)
  end

  defp print_module_summary(module) do
    %{
      "name" => name,
      "percentage" => percentage,
      "covered_lines" => covered,
      "total_lines" => total,
      "uncovered_lines" => uncovered
    } = module

    uncovered_count = length(uncovered)

    IO.puts("\n=== Module Coverage Summary ===\n")
    IO.puts("Module: #{name}")
    IO.puts("Coverage: #{percentage}%")
    IO.puts("Covered Lines: #{covered}/#{total}")
    IO.puts("Uncovered Lines: #{uncovered_count}")

    if uncovered_count > 0 do
      IO.puts("\nUncovered blocks:")

      module["uncovered_blocks"]
      |> Enum.with_index(1)
      |> Enum.each(fn {block, idx} ->
        if length(block) > 1 do
          IO.puts(
            "  Block #{idx}: lines #{List.first(block)}-#{List.last(block)} (#{length(block)} lines)"
          )
        else
          IO.puts("  Block #{idx}: line #{List.first(block)}")
        end
      end)
    end

    IO.puts("")
  end

  defp sort_modules_by_coverage(%{"source_files" => source_files}) do
    source_files
    |> Enum.map(fn file = %{"coverage" => coverage} ->
      # Calculate coverage percentage
      {covered, total} =
        coverage
        # Ignore nil entries (non-code lines)
        |> Enum.reject(&(&1 == :null))
        |> Enum.reduce({0, 0}, fn
          0, {covered, total} -> {covered, total + 1}
          _, {covered, total} -> {covered + 1, total + 1}
        end)

      percentage = if total > 0, do: covered / total * 100, else: 100.0

      # Find uncovered lines
      uncovered_lines = find_uncovered_lines(coverage)

      # Group uncovered lines into blocks
      uncovered_blocks = group_lines_into_blocks(uncovered_lines)

      Map.merge(file, %{
        "percentage" => Float.round(percentage, 1),
        "covered_lines" => covered,
        "total_lines" => total,
        "uncovered_lines" => uncovered_lines,
        "uncovered_blocks" => uncovered_blocks
      })
    end)
    |> Enum.sort_by(& &1["percentage"])
  end

  defp find_uncovered_lines(coverage) do
    coverage
    |> Enum.with_index(1)
    |> Enum.filter(fn {cov, _} -> cov == 0 end)
    |> Enum.map(fn {_, line} -> line end)
  end

  defp group_lines_into_blocks(lines) do
    lines
    |> Enum.sort()
    |> Enum.chunk_by(fn line ->
      # Find consecutive lines by checking if the previous line exists
      index = Enum.find_index(lines, &(&1 == line - 1))
      index != :null
    end)
  end

  defp print_coverage_summary_table(modules) do
    IO.puts("\n=== Module Coverage Summary (Sorted by Coverage %) ===\n")

    IO.puts(
      "#{String.pad_trailing("Idx", 5)}| #{String.pad_trailing("Coverage %", 12)}| #{String.pad_trailing("Covered/Total", 15)}| #{String.pad_trailing("Uncovered Lines", 15)}| Module"
    )

    IO.puts(
      "#{String.duplicate("-", 5)}|#{String.duplicate("-", 13)}|#{String.duplicate("-", 16)}|#{String.duplicate("-", 16)}|#{String.duplicate("-", 30)}"
    )

    modules
    |> Enum.with_index()
    |> Enum.each(fn {%{
                       "name" => name,
                       "percentage" => percentage,
                       "covered_lines" => covered,
                       "total_lines" => total,
                       "uncovered_lines" => uncovered
                     }, idx} ->
      uncovered_count = length(uncovered)

      IO.puts(
        "#{String.pad_trailing("#{idx}", 5)}| #{String.pad_trailing("#{percentage}%", 12)}| #{String.pad_trailing("#{covered}/#{total}", 15)}| #{String.pad_trailing("#{uncovered_count}", 15)}| #{name}"
      )
    end)
  end

  defp print_all_uncovered_lines(modules) do
    IO.puts("\n=== All Modules With Uncovered Lines ===\n")

    modules
    |> Enum.filter(fn %{"uncovered_lines" => lines} -> length(lines) > 0 end)
    |> Enum.each(fn %{"name" => name, "uncovered_lines" => lines, "uncovered_blocks" => blocks} ->
      IO.puts("Module: #{name}")
      IO.puts("Uncovered lines: #{length(lines)}")

      IO.puts("Uncovered blocks:")

      blocks
      |> Enum.with_index(1)
      |> Enum.each(fn {block, idx} ->
        if length(block) > 1 do
          IO.puts(
            "  Block #{idx}: lines #{List.first(block)}-#{List.last(block)} (#{length(block)} lines)"
          )
        else
          IO.puts("  Block #{idx}: line #{List.first(block)}")
        end
      end)

      IO.puts("")
    end)
  end

  defp print_detailed_module_coverage(%{"name" => name, "coverage" => coverage}) do
    IO.puts("\nDetailed Coverage for module: #{name}\n")
    IO.puts("Line | Covered?")
    IO.puts("-----|---------")

    Enum.with_index(coverage, 1)
    |> Enum.each(fn {coverage_info, line_number} ->
      status =
        case coverage_info do
          :null -> "N/A"
          0 -> "❌"
          _ -> "✅"
        end

      IO.puts("#{String.pad_leading(Integer.to_string(line_number), 4)} | #{status}")
    end)
  end
end

CoverageSummary.main(System.argv())
