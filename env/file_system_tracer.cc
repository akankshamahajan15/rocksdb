// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "env/file_system_tracer.h"

namespace ROCKSDB_NAMESPACE {

namespace {
using namespace std::chrono;
uint64_t Timestamp() {
  return duration_cast<microseconds>(system_clock::now().time_since_epoch())
      .count();
}
}  // namespace

IOStatus FileSystemTracingWrapper::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), fname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::NewDirectory(
    const std::string& name, const IOOptions& io_opts,
    std::unique_ptr<FSDirectory>* result, IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->NewDirectory(name, io_opts, result, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), name);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::GetChildren(const std::string& dir,
                                               const IOOptions& io_opts,
                                               std::vector<std::string>* r,
                                               IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->GetChildren(dir, io_opts, r, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), dir);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::DeleteFile(const std::string& fname,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->DeleteFile(fname, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), fname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::CreateDir(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->CreateDir(dirname, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), dirname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::CreateDirIfMissing(
    const std::string& dirname, const IOOptions& options, IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->CreateDirIfMissing(dirname, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), dirname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::DeleteDir(const std::string& dirname,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->DeleteDir(dirname, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileName, __func__,
                          end_time - start_time, s.ToString(), dirname);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FileSystemTracingWrapper::GetFileSize(const std::string& fname,
                                               const IOOptions& options,
                                               uint64_t* file_size,
                                               IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->GetFileSize(fname, options, file_size, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileNameAndFileSize, __func__,
                          end_time - start_time, s.ToString(), fname,
                          *file_size);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::Read(size_t n,
                                              const IOOptions& options,
                                              Slice* result, char* scratch,
                                              IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Read(n, options, result, scratch, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLen, __func__,
                          end_time - start_time, s.ToString(), result->size());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::InvalidateCache(size_t offset,
                                                         size_t length) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->InvalidateCache(offset, length);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), length, offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSSequentialFileTracingWrapper::PositionedRead(
    uint64_t offset, size_t n, const IOOptions& options, Slice* result,
    char* scratch, IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s =
      target()->PositionedRead(offset, n, options, result, scratch, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), result->size(),
                          offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::Read(uint64_t offset, size_t n,
                                                const IOOptions& options,
                                                Slice* result, char* scratch,
                                                IODebugContext* dbg) const {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), n, offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::MultiRead(FSReadRequest* reqs,
                                                     size_t num_reqs,
                                                     const IOOptions& options,
                                                     IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->MultiRead(reqs, num_reqs, options, dbg);
  uint64_t end_time = Timestamp();
  uint64_t latency = end_time - start_time;
  for (size_t i = 0; i < num_reqs; i++) {
    IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                            latency, reqs[i].status.ToString(), reqs[i].len,
                            reqs[i].offset);
    io_tracer_->WriteIOOp(io_record);
  }
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::Prefetch(uint64_t offset, size_t n,
                                                    const IOOptions& options,
                                                    IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Prefetch(offset, n, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), n, offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomAccessFileTracingWrapper::InvalidateCache(size_t offset,
                                                           size_t length) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->InvalidateCache(offset, length);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), length,
                          static_cast<uint64_t>(offset));
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Append(const Slice& data,
                                              const IOOptions& options,
                                              IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Append(data, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLen, __func__,
                          end_time - start_time, s.ToString(), data.size());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::PositionedAppend(
    const Slice& data, uint64_t offset, const IOOptions& options,
    IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->PositionedAppend(data, offset, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), data.size(),
                          offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Truncate(uint64_t size,
                                                const IOOptions& options,
                                                IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Truncate(size, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLen, __func__,
                          end_time - start_time, s.ToString(), size);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSWritableFileTracingWrapper::Close(const IOOptions& options,
                                             IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Close(options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOGeneral, __func__,
                          end_time - start_time, s.ToString());
  io_tracer_->WriteIOOp(io_record);
  return s;
}

uint64_t FSWritableFileTracingWrapper::GetFileSize(const IOOptions& options,
                                                   IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  uint64_t file_size = target()->GetFileSize(options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOFileNameAndFileSize,
                          "GetFileSize", end_time - start_time,
                          "" /* file_name */, file_size);
  io_tracer_->WriteIOOp(io_record);
  return file_size;
}

IOStatus FSWritableFileTracingWrapper::InvalidateCache(size_t offset,
                                                       size_t length) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->InvalidateCache(offset, length);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), length,
                          static_cast<uint64_t>(offset));
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Write(uint64_t offset, const Slice& data,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Write(offset, data, options, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), data.size(),
                          offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}

IOStatus FSRandomRWFileTracingWrapper::Read(uint64_t offset, size_t n,
                                            const IOOptions& options,
                                            Slice* result, char* scratch,
                                            IODebugContext* dbg) const {
  uint64_t start_time = Timestamp();
  IOStatus s = target()->Read(offset, n, options, result, scratch, dbg);
  uint64_t end_time = Timestamp();
  IOTraceRecord io_record(end_time, TraceType::kIOLenAndOffset, __func__,
                          end_time - start_time, s.ToString(), n, offset);
  io_tracer_->WriteIOOp(io_record);
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
