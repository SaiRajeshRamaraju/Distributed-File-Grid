#include <argparse/argparse.hpp>
#include <cstdlib>
#include <dfg/client/client.hpp>
#include <dfg/version.hpp>
#include <iostream>
#include <string>
#include <utility>

using dfg::client::file_delete;
using dfg::client::file_download;
using dfg::client::file_list;
using dfg::client::file_upload;
using dfg::client::run_client_test;

static std::string get_default_server_ip() {
  const char *env = std::getenv("DFG_HEAD_HOST");
  if (!env)
    env = std::getenv("DFG_SERVER_HOST");
  return env ? std::string(env) : "127.0.0.1";
}

static int get_default_server_port() {
  const char *env = std::getenv("DFG_HEAD_PORT");
  if (!env)
    env = std::getenv("DFG_SERVER_PORT");
  if (env) {
    try {
      int p = std::stoi(env);
      if (p > 0 && p <= 65535)
        return p;
    } catch (...) {
    }
  }
  return 9669;
}

static std::string extract_filename(const std::string &path) {
  size_t last_slash = path.find_last_of("/\\");
  if (last_slash != std::string::npos) {
    return path.substr(last_slash + 1);
  }
  return path;
}

static bool validate_port(int port) {
  if (port <= 0 || port > 65535) {
    std::cerr << "Invalid port number: " << port << " (must be 1-65535)"
              << std::endl;
    return false;
  }
  return true;
}

static std::pair<std::string, int>
resolve_server_target(const argparse::ArgumentParser &sub_cmd,
                      const argparse::ArgumentParser &parent) {
  std::string ip = sub_cmd.is_used("--server_ip")
                       ? sub_cmd.get<std::string>("--server_ip")
                       : (parent.is_used("--server_ip")
                              ? parent.get<std::string>("--server_ip")
                              : sub_cmd.get<std::string>("--server_ip"));
  int port = sub_cmd.is_used("--server_port")
                 ? sub_cmd.get<int>("--server_port")
                 : (parent.is_used("--server_port")
                        ? parent.get<int>("--server_port")
                        : sub_cmd.get<int>("--server_port"));
  return {ip, port};
}

int main(int argc, char *argv[]) {
  argparse::ArgumentParser program("client", APP_VERSION);
  program.add_description(
      "Distributed File Grid — Client CLI for file lifecycle operations");

  // Global / top-level flags on program
  program.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  program.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");
  program.add_argument("--upload").help("Filepath of the file to be uploaded");
  program.add_argument("--download").help("Filename of the download file");
  program.add_argument("--downlaod")
      .help("Filename of the download file (alias for backward compatibility)");
  program.add_argument("-o", "--output")
      .default_value(std::string(""))
      .help("Output path of the file to be stored");
  program.add_argument("--list").default_value(false).implicit_value(true).help(
      "List stored files");
  program.add_argument("--test").default_value(false).implicit_value(true).help(
      "Run client connectivity/functionality test");

  // Subcommand: upload
  argparse::ArgumentParser upload_cmd("upload");
  upload_cmd.add_description("Upload a local file to distributed storage");
  upload_cmd.add_argument("filepath").help("Path of local file to upload");
  upload_cmd.add_argument("filename")
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
      .help("Remote filename (optional, defaults to local basename)");
  upload_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  upload_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: download
  argparse::ArgumentParser download_cmd("download");
  download_cmd.add_description(
      "Download a file from distributed storage to local path");
  download_cmd.add_argument("filename").help("Name of the file to download");
  download_cmd.add_argument("output_path")
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
      .help("Local destination path (optional, defaults to filename)");
  download_cmd.add_argument("-o", "--output")
      .default_value(std::string(""))
      .help("Local destination path");
  download_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  download_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: list
  argparse::ArgumentParser list_cmd("list");
  list_cmd.add_description(
      "List all files stored across the Distributed File Grid");

  // Subcommand: delete
  argparse::ArgumentParser delete_cmd("delete");
  delete_cmd.add_description("Delete a file from the distributed storage");
  delete_cmd.add_argument("filename").help("Name of the file to delete");
  delete_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  delete_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  list_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  list_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: test
  argparse::ArgumentParser test_cmd("test");
  test_cmd.add_description(
      "Run end-to-end client connectivity and integrity test");
  test_cmd.add_argument("--server_ip", "--si", "-s")
      .default_value(get_default_server_ip())
      .help("Head server IP address");
  test_cmd.add_argument("--server_port", "--sp", "-p")
      .default_value(get_default_server_port())
      .scan<'i', int>()
      .help("Head server port");

  // Subcommand: help
  argparse::ArgumentParser help_cmd("help");
  help_cmd.add_description("Show help message");

  // Subcommand: version
  argparse::ArgumentParser version_cmd("version");
  version_cmd.add_description("Show version information");

  program.add_subparser(upload_cmd);
  program.add_subparser(download_cmd);

  program.add_subparser(list_cmd);
  program.add_subparser(delete_cmd);

  program.add_subparser(test_cmd);
  program.add_subparser(help_cmd);
  program.add_subparser(version_cmd);

  if (argc < 2) {
    std::cout << program << std::endl;
    return 1;
  }

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    std::cerr << "Error: " << err.what() << std::endl;
    std::cerr << program << std::endl;
    return 1;
  }

  if (program.is_subcommand_used(help_cmd)) {
    std::cout << program << std::endl;
    return 0;
  }

  if (program.is_subcommand_used(version_cmd)) {
    std::cout << "Distributed File Grid Client version " << APP_VERSION
              << std::endl;
    return 0;
  }

  if (program.is_subcommand_used(delete_cmd)) {

    auto [server_ip, server_port] = resolve_server_target(delete_cmd, program);
    if (!validate_port(server_port))
      return 1;
    std::string filename = delete_cmd.get<std::string>("filename");
    return file_delete(server_ip, server_port, filename);
  }

  if (program.is_subcommand_used(upload_cmd)) {

    auto [server_ip, server_port] = resolve_server_target(upload_cmd, program);
    if (!validate_port(server_port))
      return 1;

    std::string filepath = upload_cmd.get<std::string>("filepath");
    std::string filename = upload_cmd.get<std::string>("filename");
    if (filename.empty()) {
      filename = extract_filename(filepath);
    }
    return file_upload(server_ip, server_port, filepath, filename);
  }

  if (program.is_subcommand_used(download_cmd)) {
    auto [server_ip, server_port] =
        resolve_server_target(download_cmd, program);
    if (!validate_port(server_port))
      return 1;

    std::string filename = download_cmd.get<std::string>("filename");
    std::string output_path = download_cmd.get<std::string>("output_path");
    if (output_path.empty()) {
      if (download_cmd.is_used("-o")) {
        output_path = download_cmd.get<std::string>("-o");
      }
    }
    if (output_path.empty()) {
      output_path = filename;
    }
    return file_download(server_ip, server_port, filename, output_path);
  }

  if (program.is_subcommand_used(list_cmd)) {
    auto [server_ip, server_port] = resolve_server_target(list_cmd, program);
    if (!validate_port(server_port))
      return 1;
    return file_list(server_ip, server_port);
  }

  if (program.is_subcommand_used(test_cmd)) {
    auto [server_ip, server_port] = resolve_server_target(test_cmd, program);
    if (!validate_port(server_port))
      return 1;
    return run_client_test(server_ip, server_port);
  }

  // Top-level flag execution
  std::string server_ip = program.get<std::string>("--server_ip");
  int server_port = program.get<int>("--server_port");
  if (!validate_port(server_port))
    return 1;

  if (program.get<bool>("--list")) {
    return file_list(server_ip, server_port);
  }

  if (program.get<bool>("--test")) {
    return run_client_test(server_ip, server_port);
  }

  if (auto upload_file = program.present<std::string>("--upload")) {
    std::string file_path = *upload_file;
    std::string file_name = extract_filename(file_path);
    return file_upload(server_ip, server_port, file_path, file_name);
  }

  std::string download_file;
  if (auto opt = program.present<std::string>("--download")) {
    download_file = *opt;
  } else if (auto opt = program.present<std::string>("--downlaod")) {
    download_file = *opt;
  }

  if (!download_file.empty()) {
    std::string output_path = download_file;
    if (program.is_used("-o")) {
      output_path = program.get<std::string>("-o");
    }
    return file_download(server_ip, server_port, download_file, output_path);
  }

  std::cerr << "Error: No command or operation flag specified.\n" << std::endl;
  std::cerr << program << std::endl;
  return 1;
}
