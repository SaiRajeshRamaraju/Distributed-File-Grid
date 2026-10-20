#include "chunk_storage.hpp"
#include <dfg/atomic_output.hpp>
#include <gtest/gtest.h>
#include <fstream>

namespace {
class StorageTest : public ::testing::Test {
protected:
  void SetUp() override {
    char pattern[] = "/tmp/dfg-storage-XXXXXX";
    const auto directory = ::mkdtemp(pattern);
    ASSERT_NE(directory, nullptr);
    root = directory;
  }
  void TearDown() override { fs::remove_all(root); }
  fs::path root;
};

TEST_F(StorageTest, StoredChunksSurviveRecreationOfTheStorageEngine) {
  const std::vector<char> data{'a', '\0', 'b'};
  {
    ChunkStorage storage(root.string());
    ASSERT_TRUE(storage.async_store_chunk("under_scores.bin_chunk_0", data).get().first);
  }
  ChunkStorage restarted(root.string());
  EXPECT_EQ(restarted.async_retrieve_chunk("under_scores.bin_chunk_0").get(), data);
}

TEST_F(StorageTest, RejectsPathsOutsideStorageAndUnsafeChunkNames) {
  fs::create_directory(root / "chunks");
  std::ofstream(root / "secret") << "secret";
  fs::create_symlink(root / "secret", root / "chunks" / "link");
  ChunkStorage storage((root / "chunks").string());
  EXPECT_TRUE(storage.async_retrieve_chunk("unknown", (root / "secret").string()).get().empty());
  EXPECT_TRUE(storage.async_retrieve_chunk("unknown", (root / "chunks" / "link").string()).get().empty());
  EXPECT_FALSE(storage.async_store_chunk("../../escape", {'x'}).get().first);
}

TEST_F(StorageTest, FailedDeletionKeepsRegistryAndRetryIsIdempotent) {
  ChunkStorage storage(root.string());
  auto result = storage.async_store_chunk("test_chunk_0", {'x'}).get();
  ASSERT_TRUE(result.first);
  fs::rename(result.second, root / "saved");
  fs::create_directory(result.second);
  std::ofstream(fs::path(result.second) / "child") << "busy";
  EXPECT_FALSE(storage.delete_chunk("test_chunk_0"));
  EXPECT_EQ(storage.list_chunks().size(), 1);
  fs::remove_all(result.second);
  fs::rename(root / "saved", result.second);
  EXPECT_TRUE(storage.delete_chunk("test_chunk_0"));
  EXPECT_TRUE(storage.delete_chunk("test_chunk_0"));
  EXPECT_FALSE(fs::exists(result.second));
}

TEST_F(StorageTest, FailedChunkWriteDoesNotPublishARegistryEntry) {
  const auto directory = root / "chunks";
  ChunkStorage storage(directory.string());
  fs::remove(directory);
  std::ofstream(directory) << "directory became a file";
  EXPECT_FALSE(storage.async_store_chunk("failed_chunk_0", {'x'}).get().first);
  EXPECT_TRUE(storage.list_chunks().empty());
}

TEST_F(StorageTest, AbandonedOutputPreservesExistingFile) {
  const auto path = root / "output";
  std::ofstream(path) << "original";
  {
    dfg::AtomicOutput output(path);
    output.stream() << "incomplete";
  }
  std::string contents;
  std::ifstream(path) >> contents;
  EXPECT_EQ(contents, "original");
  EXPECT_EQ(std::distance(fs::directory_iterator(root), fs::directory_iterator{}), 1);
  {
    dfg::AtomicOutput output(path);
    output.stream() << "replacement";
    output.commit();
  }
  std::ifstream(path) >> contents;
  EXPECT_EQ(contents, "replacement");
}
} // namespace
