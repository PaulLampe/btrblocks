#include <aws/core/Aws.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "common/Utils.hpp"
#include "compression/Datablock.hpp"

using namespace std;
using namespace Aws;
using namespace Aws::Utils;

const string out_dir_name = "decompression-bench-dataset/";
const string s3_bucket = "public-bi-eu-central-1";
const string s3_region = "eu-central-1";
const string prefix = "/v0.0.1/btr/Medicare1_1/";

void DownloadBenchmarkDataset(const Aws::String& bucket_name,
                              const Aws::String& objectKey,
                              const Aws::String& local_path,
                              const shared_ptr<Aws::Transfer::TransferManager>& transfer_manager) {
  auto downloadHandle = transfer_manager->DownloadFile(bucket_name, objectKey, [&local_path]() {
    auto* stream = Aws::New<Aws::FStream>("s3file", local_path, std::ios_base::out);
    stream->rdbuf()->pubsetbuf(nullptr, 0);

    return stream;
  });

  downloadHandle->WaitUntilFinished();
  auto downStat = downloadHandle->GetStatus();
  if (downStat != Transfer::TransferStatus::COMPLETED) {
    auto err = downloadHandle->GetLastError();
    std::cout << "\nFile download failed:  " << err.GetMessage() << '\n';
  }
  std::cout << "\nFile download to " << local_path << " finished." << '\n';
}

bool FileExists(string path) {
  ifstream f;
  try {
    f.open(path);
    return f.good();
  } catch (int _) {
    return false;
  }
}

int main(int argc, char** argv) {
  std::filesystem::create_directory(out_dir_name);

  Aws::SDKOptions options;
  Aws::InitAPI(options);
  {
    const Aws::String bucket_name = s3_bucket;
    const Aws::String region = s3_region;

    Aws::Client::ClientConfiguration config;
    config.httpRequestTimeoutMs = 300000;
    config.requestTimeoutMs = 300000;
    config.connectTimeoutMs = 300000;

    if (!region.empty()) {
      config.region = region;
    }

    // snippet-start:[transfer-manager.cpp.transferOnStream.code]
    auto s3_client = std::make_shared<Aws::S3::S3Client>(config);
    auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("executor", 8);

    Aws::Transfer::TransferManagerConfiguration transfer_config(executor.get());
    transfer_config.s3Client = s3_client;

    transfer_config.downloadProgressCallback =
        [](const Aws::Transfer::TransferManager*,
           const std::shared_ptr<const Aws::Transfer::TransferHandle>& handle) {
          std::cout << "\r"
                    << "Benchmark Download Progress for " << handle->GetKey() << ": "
                    << handle->GetBytesTransferred() << " of " << handle->GetBytesTotalSize()
                    << " bytes" << std::flush;
        };

    auto transfer_manager = Aws::Transfer::TransferManager::Create(transfer_config);

    auto t1 = std::chrono::high_resolution_clock::now();

    auto metaKey = prefix + "metadata";
    auto metaLocalPath = out_dir_name + "metadata";

    if (!FileExists(metaLocalPath)) {
      DownloadBenchmarkDataset(bucket_name, metaKey, metaLocalPath, transfer_manager);
    }

    std::vector<char> raw_file_metadata;
    const btrblocks::FileMetadata* file_metadata;
    {
      btrblocks::Utils::readFileToMemory(metaLocalPath, raw_file_metadata);
      file_metadata = reinterpret_cast<const btrblocks::FileMetadata*>(raw_file_metadata.data());

      vector<string> columnKeys{};

      for (btrblocks::u32 column_i = 0; column_i < file_metadata->num_columns; column_i++) {
        for (btrblocks::u32 part_i = 0; part_i < file_metadata->parts[column_i].num_parts;
             part_i++) {
          columnKeys.push_back("column" + to_string(column_i) + "_part" + to_string(part_i));
        }
      }

      for (auto& columnKey : columnKeys) {
        
        auto objectKey = prefix + columnKey;
        auto objectLocalPath = out_dir_name + columnKey;


        if (!FileExists(objectLocalPath)) {
          DownloadBenchmarkDataset(bucket_name, objectKey, objectLocalPath, transfer_manager);
        } else {
          cout << "File: " << objectLocalPath << " already exists \n";
        }
      }
    }

    auto t2 = std::chrono::high_resolution_clock::now();

    auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    std::cout << "Download took " << ms_int.count() << "ms\n";
  }
  Aws::ShutdownAPI(options);

  return 0;
}  // namespace btrbench