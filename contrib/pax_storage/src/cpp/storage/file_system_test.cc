#include <gtest/gtest.h>

#include "comm/singleton.h"
#include "storage/local_file_system.h"

namespace pax::tests {
#define PAX_TEST_CMD_LENGTH 2048
#define PAX_TEST_LIST_FILE_NUM 10

class LocalFileSystemTest : public ::testing::Test {
 public:
  void SetUp() override {
    remove(file_name_.c_str());
    remove(file_path_.c_str());
  }

  void TearDown() override {
    remove(file_name_.c_str());
    remove(file_path_.c_str());
  }

 protected:
  const std::string file_name_ = "./test.file";
  const std::string file_path_ = "./test_path";
  const std::string file_full_path_ =
      "./test_path/16400/GPDB_1_302206171/13261/16394";
};

TEST_F(LocalFileSystemTest, Open) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  EXPECT_NE(nullptr, file_ptr);

  file_ptr->Close();
  delete file_ptr;
}

TEST_F(LocalFileSystemTest, BuildPath) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  ASSERT_NE(nullptr, file_ptr);

  auto path = local_fs->BuildPath(file_ptr);
  ASSERT_EQ(path, "./test.file");

  file_ptr->Close();
  delete file_ptr;
}

TEST_F(LocalFileSystemTest, WriteRead) {
  auto local_fs = pax::Singleton<LocalFileSystem>::GetInstance();
  ASSERT_NE(nullptr, local_fs);

  auto file_ptr = local_fs->Open(file_name_, fs::kWriteMode);
  ASSERT_NE(nullptr, file_ptr);

  auto write_size = file_ptr->Write("abc", 3);
  ASSERT_EQ(3, write_size);

  file_ptr->Flush();
  file_ptr->Close();
  file_ptr = local_fs->Open(file_name_, fs::kReadMode);
  ASSERT_NE(nullptr, file_ptr);

  char buff[10] = {0};
  auto read_size = file_ptr->Read(buff, 3);
  ASSERT_EQ(3, read_size);
  ASSERT_EQ(strncmp("abc", buff, 3), 0);
}

TEST_F(LocalFileSystemTest, ListDirectory) {
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);

  ASSERT_EQ(0, fs->CreateDirectory(file_path_));
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    std::string path;
    path.append(file_path_);
    path.append("/test");
    path.append(std::to_string(i));
    File *f = fs->Open(path, fs::kWriteMode);
    f->Close();
  }

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), static_cast<size_t>(PAX_TEST_LIST_FILE_NUM));
}

TEST_F(LocalFileSystemTest, CopyFile) {
  CBDB_TRY();
  {
    static const char *pax_copy_test_dir = "./copytest";
    static const char *pax_copy_src_path = file_name_.c_str();
    static const char *pax_copy_dst_path = "./copytest/test_dst";

    int result = 0;
    FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

    fs->DeleteDirectory(pax_copy_test_dir, true);
    ASSERT_NE(access(pax_copy_test_dir, F_OK), 0);

    cbdb::MakedirRecursive(pax_copy_test_dir);
    File *src_file = fs->Open(pax_copy_src_path, pax::fs::kWriteMode);

    src_file->Write("abc", 3);
    src_file->Flush();
    src_file->Close();

    src_file = fs->Open(pax_copy_src_path, pax::fs::kReadMode);

    File *dst_file = fs->Open(pax_copy_dst_path, pax::fs::kWriteMode);

    InitFileAccess();
    fs->CopyFile(src_file, dst_file);

    src_file->Close();
    dst_file->Close();

    result = access(pax_copy_dst_path, F_OK);
    ASSERT_NE(result, -1);

    struct stat src_stat, dst_stat;

    stat(pax_copy_src_path, &src_stat);
    stat(pax_copy_dst_path, &dst_stat);
    ASSERT_EQ(src_stat.st_size, dst_stat.st_size);

    fs->DeleteDirectory(pax_copy_test_dir, true);
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({});
  CBDB_END_TRY();
}

TEST_F(LocalFileSystemTest, MakedirRecursive) {
  int result = 0;
  struct stat st {};
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  fs->DeleteDirectory(file_full_path_, true);
  ASSERT_NE(access(file_full_path_.c_str(), F_OK), 0);

  cbdb::MakedirRecursive(file_full_path_.c_str());
  result = stat(file_full_path_.c_str(), &st);
  ASSERT_EQ(result, 0);
}

TEST_F(LocalFileSystemTest, CreateDeleteDirectory) {
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);

  ASSERT_EQ(0, fs->CreateDirectory(file_path_));
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    std::string path;
    path.append(file_path_);
    path.append("/test");
    path.append(std::to_string(i));
    File *f = fs->Open(path, fs::kWriteMode);
    f->Close();
  }

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), static_cast<size_t>(PAX_TEST_LIST_FILE_NUM));

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);
}

TEST_F(LocalFileSystemTest, DeleteDirectoryReserveToplevel) {
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  std::vector<std::string> filelist;

  fs->DeleteDirectory(file_path_, true);
  ASSERT_NE(access(file_path_.c_str(), F_OK), 0);

  ASSERT_EQ(0, fs->CreateDirectory(file_path_));
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  for (int i = 0; i < PAX_TEST_LIST_FILE_NUM; i++) {
    std::string path;
    path.append(file_path_);
    path.append("/test");
    path.append(std::to_string(i));
    File *f = fs->Open(path, fs::kWriteMode);
    f->Close();
  }

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), static_cast<size_t>(PAX_TEST_LIST_FILE_NUM));

  fs->DeleteDirectory(file_path_, false);
  ASSERT_EQ(access(file_path_.c_str(), F_OK), 0);

  filelist = fs->ListDirectory(file_path_);
  ASSERT_EQ(filelist.size(), 0UL);
}
}  // namespace pax::tests
