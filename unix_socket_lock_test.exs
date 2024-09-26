# Caveats:
#
#   1. Unix domain socket implementations impose max length on the
#      path, around 104, see https://unix.stackexchange.com/a/367012.
#      This makes them brittle to use, even with tmpdir, because on
#      macOS the tmpdir path is long.
#
#   2. OTP :socket is emulated on Windows using Async I/O, but it had
#      a critical bug, see https://github.com/erlang/otp/issues/8853.
#      It has been fixed in OTP 27.1.1, and will get patched for OTP 26.

defmodule Lock do
  @moduledoc false

  # Lock implementation working across multiple OS processes.
  #
  # The lock is implemented using Unix domain sockets and hard links.
  #
  # When using Unix domain sockets, we need to be aware of two inherent
  # sources of race conditions:
  #
  #   1. When a socket is closed, the file is left in the file system
  #      and considered "stale". In order for another process to bind
  #      to the same path, the file needs to be removed. However, we
  #      need to guarantee only a single process can remove the file,
  #      otherwise a process may end up removing a newly bound file.
  #
  #   2. Listening on the path is not atomic, that is, when a process
  #      is between `:socket.bind/2` and `:socket.listen/2` calls,
  #      connecting to the socket returns `:econnrefused`, making it
  #      indistinguishable from a "stale" socket.
  #
  # To address 2. we first bind to a random socket_* file and start
  # listening, then we attempt hard links to lock_* files. This approach
  # guarantees that a lock_* socket either accept connections or is
  # stale.
  #
  # To address 1. we employ a chained locking procedure. Specifically,
  # we attempt to link our socket to lock_0, if that fails, we try to
  # connect to lock_0 socket. If we manage to connect, it means the
  # lock is taken, so we wait for it to close and start over. If we
  # fail to connect, it means the lock is stale, so we want to replace
  # it. In order to do that, we try to obtain lock_1. Again, we try
  # to link and connect. Eventually, we successfully link at lock_N.
  # At that point we can clean up all the files, so we do these steps:
  #
  #   * move our socket to lock_0
  #   * remove all the other socket_* files
  #   * remove all lock_1+ files
  #
  # It is important to perform these steps in this order, to avoid
  # race conditions. By moving to lock_0, we make sure that all new
  # processes trying to lock will connect to that file. By removing
  # all socket_* files we make sure that currently paused processes
  # that are about to link at lock_N will fail to link, since the
  # socket_* will no longer exist (once lock_N is removed).
  #
  # Finally, note that we do not remove the lock file in `unlock/1`.
  # If we did that, another process could try to connect and fail
  # because the file does not exist, however the process would assume
  # the file is stale and needs to be replaced, therefore possibly
  # replacing another process who successfully links to the empty
  # spot.

  @opaque t :: %{socket: :socket.socket()}

  @doc """
  Acquires a lock identified by the given path.

  This function blocks until the lock is acquired by this process.
  """
  @spec lock(String.t()) :: t()
  def lock(path) do
    :ok = File.mkdir_p(path)

    socket_path = Path.join(path, "socket_#{random_id()}")

    {:ok, socket} = :socket.open(:local, :stream)
    :ok = :socket.bind(socket, %{family: :local, path: socket_path})
    :ok = :socket.listen(socket, 128)

    case grab_lock(path, socket_path, 0) do
      {:ok, 0} ->
        # We grabbed lock_0, so all good
        %{socket: socket}

      {:ok, _n} ->
        # We grabbed lock_1+, so we replace lock_0 and clean up
        take_over(path, socket_path)
        %{socket: socket}

      {:taken, probe_socket} ->
        # Another process has the lock, wait for close and start over
        remove_socket(socket_path, socket)
        await_close(probe_socket)
        lock(path)

      :invalidated ->
        remove_socket(socket_path, socket)
        lock(path)
    end
  end

  defp grab_lock(path, socket_path, n) do
    lock_path = Path.join(path, "lock_#{n}")

    case File.ln(socket_path, lock_path) do
      :ok ->
        {:ok, n}

      {:error, :eexist} ->
        case probe(path, lock_path) do
          {:ok, probe_socket} ->
            {:taken, probe_socket}

          :error ->
            grab_lock(path, socket_path, n + 1)
        end

      {:error, :enoent} ->
        :invalidated
    end
  end

  defp probe(path, socket_path) do
    {:ok, socket} = :socket.open(:local, :stream)

    # On Windows, we always need to bind ourselves before connecting
    tmp_path = Path.join(path, "socket_#{random_id()}")
    :ok = :socket.bind(socket, %{family: :local, path: tmp_path})
    _ = File.rm(tmp_path)

    case :socket.connect(socket, %{family: :local, path: socket_path}) do
      :ok ->
        {:ok, socket}

      {:error, _reason} ->
        :socket.close(socket)
        :error
    end
  end

  defp take_over(path, socket_path) do
    lock_path = Path.join(path, "lock_0")

    # We linked to lock_N successfully, so port_path should exist
    :ok = File.rename(socket_path, lock_path)

    {:ok, names} = File.ls(path)

    for "socket_" <> _ = name <- names do
      _ = File.rm(Path.join(path, name))
    end

    for "lock_" <> _ = name <- names, name != "lock_0" do
      _ = File.rm(Path.join(path, name))
    end
  end

  defp remove_socket(socket_path, socket) do
    _ = File.rm(socket_path)
    :socket.close(socket)
  end

  defp await_close(socket) do
    {:error, _reason} = :socket.recv(socket)
    # Make sure the socket is closed to avoid warnings on Windows
    :socket.close(socket)
  end

  defp random_id() do
    :crypto.strong_rand_bytes(12) |> Base.url_encode64()
  end

  @doc """
  Releases lock acquired via `lock/1`.
  """
  @spec unlock(t()) :: :ok
  def unlock(lock) do
    :socket.close(lock.socket)
    :ok
  end
end

ExUnit.start()

defmodule LockTest do
  use ExUnit.Case, async: true

  @tag :tmp_dir
  test "number increment", %{tmp_dir: tmp_dir} do
    n = 50
    number_path = Path.join(tmp_dir, "number.txt")
    # Note: we use system tmpdir, because the test one has too long path
    lock_path = Path.join(System.tmp_dir!(), "test_lock")

    File.write!(number_path, "0")

    refs =
      for _ <- 1..n do
        spawn_monitor(fn ->
          lock = Lock.lock(lock_path)

          number = number_path |> File.read!() |> String.to_integer()
          new_number = number + 1
          File.write!(number_path, Integer.to_string(new_number))

          assert File.read!(number_path) == Integer.to_string(new_number)

          # Terminate without unlocking in random cases
          case Enum.random(1..2) do
            1 -> Lock.unlock(lock)
            2 -> :ok
          end
        end)
        |> elem(1)
      end

    await_monitors(refs)

    assert File.read!(number_path) == Integer.to_string(n)
  end

  defp await_monitors([]), do: :ok

  defp await_monitors(refs) do
    receive do
      {:DOWN, ref, _, _, _} -> await_monitors(refs -- [ref])
    end
  end
end
