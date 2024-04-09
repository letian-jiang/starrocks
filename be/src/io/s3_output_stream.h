// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <fmt/format.h>

#include "common/logging.h"

#include "io/output_stream.h"

namespace starrocks::io {

class S3OutputStream : public OutputStream {
public:
    explicit S3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string object,
                            int64_t max_single_part_size, int64_t min_upload_part_size);

    ~S3OutputStream() override = default;

    // Disallow copy and assignment
    S3OutputStream(const S3OutputStream&) = delete;
    void operator=(const S3OutputStream&) = delete;

    // Disallow move, because no usage now
    S3OutputStream(S3OutputStream&&) = delete;
    void operator=(S3OutputStream&&) = delete;

    Status write(const void* data, int64_t size) override;

    [[nodiscard]] bool allows_aliasing() const override { return false; }

    Status write_aliased(const void* data, int64_t size) override;

    Status skip(int64_t count) override;

    StatusOr<Buffer> get_direct_buffer() override;

    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override;

    Status close() override;

private:
    Status create_multipart_upload();
    Status multipart_upload();
    Status singlepart_upload();
    Status complete_multipart_upload();

    std::shared_ptr<Aws::S3::S3Client> _client;
    const Aws::String _bucket;
    const Aws::String _object;
    const int64_t _max_single_part_size;
    const int64_t _min_upload_part_size;
    Aws::String _buffer;
    Aws::String _upload_id;
    std::vector<Aws::String> _etags;
};

class S3DirectOutputStream : public DirectOutputStream {
public:
    S3DirectOutputStream(std::shared_ptr <Aws::S3::S3Client> client, std::string bucket, std::string object)
            : _client(client), _bucket(std::move(bucket)), _object(std::move(object)) {}

    Status init() override {
        Aws::S3::Model::CreateMultipartUploadRequest req;
        req.SetBucket(_bucket);
        req.SetKey(_object);
        Aws::S3::Model::CreateMultipartUploadOutcome outcome = _client->CreateMultipartUpload(req);
        if (outcome.IsSuccess()) {
            _upload_id = outcome.GetResult().GetUploadId();
            return Status::OK();
        }
        return Status::IOError(
                fmt::format("S3: Fail to create multipart upload for object {}/{}: {}", _bucket, _object,
                            outcome.GetError().GetMessage()));
    }

    // TODO: make `write` thread-safe for concurrent calls
    Status write(const uint8_t *data, size_t size) override {
        Aws::S3::Model::UploadPartRequest req;
        req.SetBucket(_bucket);
        req.SetKey(_object);
        req.SetPartNumber(static_cast<int>(_etags.size() + 1));
        req.SetUploadId(_upload_id);
        req.SetContentLength(static_cast<int64_t>(_buffer.size()));
        req.SetBody(std::make_shared<Aws::StringStream>(_buffer));
        auto outcome = _client->UploadPart(req);
        if (outcome.IsSuccess()) {
            _etags.push_back(outcome.GetResult().GetETag());
            return Status::OK();
        }
        return Status::IOError(
                fmt::format("S3: Fail to upload part of {}/{}: {}", _bucket, _object,
                            outcome.GetError().GetMessage()));
    }

    Status close() override {
        VLOG(12) << "Completing multipart upload s3://" << _bucket << "/" << _object;
        DCHECK(!_upload_id.empty());
        DCHECK(!_etags.empty());
        if (UNLIKELY(_etags.size() > std::numeric_limits<int>::max())) {
            return Status::NotSupported("Too many S3 upload parts");
        }
        Aws::S3::Model::CompleteMultipartUploadRequest req;
        req.SetBucket(_bucket);
        req.SetKey(_object);
        req.SetUploadId(_upload_id);
        Aws::S3::Model::CompletedMultipartUpload multipart_upload;
        for (int i = 0, sz = static_cast<int>(_etags.size()); i < sz; ++i) {
            Aws::S3::Model::CompletedPart part;
            multipart_upload.AddParts(part.WithETag(_etags[i]).WithPartNumber(i + 1));
        }
        req.SetMultipartUpload(multipart_upload);
        auto outcome = _client->CompleteMultipartUpload(req);
        if (outcome.IsSuccess()) {
            return Status::OK();
        }
        std::string error_msg = fmt::format("S3: Fail to complete multipart upload for object {}/{}, msg: {}",
                                            _bucket,
                                            _object, outcome.GetError().GetMessage());
        LOG(WARNING) << error_msg;
        return Status::IOError(error_msg);
    }

private:
    std::shared_ptr <Aws::S3::S3Client> _client;
    const Aws::String _bucket;
    const Aws::String _object;
    Aws::String _buffer;
    Aws::String _upload_id;
    std::vector <Aws::String> _etags;
};

} // namespace starrocks::io
