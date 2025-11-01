defmodule JobRunnerTest do
  use ExUnit.Case
  doctest JobRunner

  test "greets the world" do
    assert JobRunner.hello() == :world
  end
end
