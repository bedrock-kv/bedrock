defmodule Bedrock.ControlPlane.ClusterController.Recovery.DefiningProxiesAndResolvers do
  def define_proxies(n_proxies) do
    for _ <- 1..n_proxies, do: define_proxy()
  end

  def define_proxy() do
  end
end
