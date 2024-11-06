defmodule Bedrock.Internal.Time.Interval do
  @units [:microsecond, :millisecond, :second, :minute, :hour, :day]
  @type unit :: :microsecond | :millisecond | :second | :minute | :hour | :day

  @type t :: {n :: non_neg_integer(), unit()}

  def from(n, unit) when n >= 0 and unit in @units, do: {n, unit}

  def between(%DateTime{} = start, %DateTime{} = stop),
    do: from(abs(DateTime.diff(stop, start, :millisecond)), :millisecond)

  # Simplify to the largest unit possible
  def simplify({us, :microsecond}) when us >= 1000, do: {div(us, 1000), :millisecond}
  def simplify({ms, :millisecond}) when ms >= 1000, do: {div(ms, 1000), :second}
  def simplify({s, :second}) when s >= 60, do: {div(s, 60), :minute}
  def simplify({m, :minute}) when m >= 60, do: {div(m, 60), :hour}
  def simplify({h, :hour}) when h >= 24, do: {div(h, 24), :day}

  # Simplify to the smallest unit possible
  def simplify({d, :day}) when d < 1.0, do: {Kernel.*(d, 24), :hour}
  def simplify({h, :hour}) when h < 1.0, do: {Kernel.*(h, 60), :minute}
  def simplify({m, :minute}) when m < 1.0, do: {Kernel.*(m, 60), :second}
  def simplify({s, :second}) when s < 1.0, do: {Kernel.*(s, 1000), :millisecond}
  def simplify({ms, :millisecond}) when ms < 1.0, do: {Kernel.*(ms, 1000), :microsecond}

  # Perfect
  def simplify({n, unit}) when is_float(n), do: {Float.round(n, 2), unit}
  def simplify({n, _unit} = perfect) when is_integer(n), do: perfect

  def humanize(i), do: i |> simplify() |> do_humanize()

  defp do_humanize({n, unit}) when is_integer(n), do: "#{n}#{to_abbreviated_string(unit)}"

  defp do_humanize({n, unit}) when is_float(n),
    do: "#{:io_lib.format("~.2f", [n]) |> List.to_string()}#{to_abbreviated_string(unit)}"

  def to_abbreviated_string(:microsecond), do: "μs"
  def to_abbreviated_string(:millisecond), do: "ms"
  def to_abbreviated_string(:second), do: "s"
  def to_abbreviated_string(:minute), do: "m"
  def to_abbreviated_string(:hour), do: "h"
  def to_abbreviated_string(:day), do: "d"
end
