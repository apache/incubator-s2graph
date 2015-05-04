package org.hbase.async;

public class RpcTimeoutException extends RecoverableException {

  final HBaseRpc timedout_rpc;

  RpcTimeoutException(HBaseRpc rpc) {
    super("RPC Timed out " + rpc);
    this.timedout_rpc = rpc;
  }

  public HBaseRpc getTimedOutRpc() {
    return timedout_rpc;
  }

  private static final long serialVersionUID = -4253496236526944416L;

}
