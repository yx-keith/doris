// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/index/ann/ann_index_writer.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>

#include "common/cast_set.h"
#include "common/config.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"

namespace doris::segment_v2 {
static std::string get_or_default(const std::map<std::string, std::string>& properties,
                                  const std::string& key, const std::string& default_value) {
    auto it = properties.find(key);
    if (it != properties.end()) {
        return it->second;
    }
    return default_value;
}

// Project the FAISS build parameters down to the FAISS-free subset the memory
// estimator needs. Done here (inside the ANN translation unit, which has the
// FAISS include path) so ann_index_writer.h can stay FAISS-free.
static AnnBuildMemoryParams to_memory_params(const FaissBuildParameter& p) {
    AnnBuildMemoryParams m;
    switch (p.index_type) {
    case FaissBuildParameter::IndexType::HNSW:
        m.index_type = AnnBuildMemoryParams::IndexKind::HNSW;
        break;
    case FaissBuildParameter::IndexType::IVF:
        m.index_type = AnnBuildMemoryParams::IndexKind::IVF;
        break;
    case FaissBuildParameter::IndexType::IVF_ON_DISK:
        m.index_type = AnnBuildMemoryParams::IndexKind::IVF_ON_DISK;
        break;
    }
    switch (p.quantizer) {
    case FaissBuildParameter::Quantizer::FLAT:
        m.quantizer = AnnBuildMemoryParams::Quantizer::FLAT;
        break;
    case FaissBuildParameter::Quantizer::SQ4:
        m.quantizer = AnnBuildMemoryParams::Quantizer::SQ4;
        break;
    case FaissBuildParameter::Quantizer::SQ8:
        m.quantizer = AnnBuildMemoryParams::Quantizer::SQ8;
        break;
    case FaissBuildParameter::Quantizer::PQ:
        m.quantizer = AnnBuildMemoryParams::Quantizer::PQ;
        break;
    }
    m.dim = p.dim;
    m.max_degree = p.max_degree;
    m.ivf_nlist = p.ivf_nlist;
    m.pq_m = p.pq_m;
    return m;
}

AnnIndexColumnWriter::AnnIndexColumnWriter(IndexFileWriter* index_file_writer,
                                           const TabletIndex* index_meta)
        : _index_file_writer(index_file_writer), _index_meta(index_meta) {}

AnnIndexColumnWriter::~AnnIndexColumnWriter() = default;

size_t AnnIndexColumnWriter::streaming_chunk_rows(size_t dim) {
    const size_t row_bytes = dim * sizeof(float);
    if (row_bytes == 0) {
        return 1;
    }
    return std::max<size_t>(1, cast_set<size_t>(config::ann_index_build_chunk_bytes) / row_bytes);
}

Status AnnIndexColumnWriter::init() {
    Result<std::shared_ptr<DorisFSDirectory>> compound_dir = _index_file_writer->open(_index_meta);

    if (!compound_dir.has_value()) {
        return Status::IOError("Failed to open index file: {}", compound_dir.error().to_string());
    }

    _dir = compound_dir.value();

    _vector_index = nullptr;
    const auto& properties = _index_meta->properties();
    const std::string index_type = get_or_default(properties, INDEX_TYPE, "hnsw");
    const std::string metric_type = get_or_default(properties, METRIC_TYPE, "l2_distance");
    const std::string quantizer = get_or_default(properties, QUANTIZER, "flat");
    FaissBuildParameter build_parameter;
    std::shared_ptr<FaissVectorIndex> faiss_index = std::make_shared<FaissVectorIndex>();
    build_parameter.index_type = FaissBuildParameter::string_to_index_type(index_type);
    build_parameter.dim = std::stoi(get_or_default(properties, DIM, "512"));
    build_parameter.max_degree = std::stoi(get_or_default(properties, MAX_DEGREE, "32"));
    build_parameter.metric_type = FaissBuildParameter::string_to_metric_type(metric_type);
    build_parameter.ef_construction = std::stoi(get_or_default(properties, EF_CONSTRUCTION, "40"));
    build_parameter.ivf_nlist = std::stoi(get_or_default(properties, NLIST, "1024"));
    build_parameter.quantizer = FaissBuildParameter::string_to_quantizer(quantizer);
    build_parameter.pq_m = std::stoi(get_or_default(properties, PQ_M, "8"));
    build_parameter.pq_nbits = std::stoi(get_or_default(properties, PQ_NBITS, "8"));

    faiss_index->build(build_parameter);

    _vector_index = faiss_index;
    _build_params = to_memory_params(build_parameter);

    LOG_INFO(
            "Create a new faiss index, index_id {} index_type {} dim {} metric_type {} "
            "max_degree {}, ef_construction {}, quantizer {}",
            _index_meta->index_id(), index_type, build_parameter.dim, metric_type,
            build_parameter.max_degree, build_parameter.ef_construction, quantizer);

    return _acquire_memory_budget();
}

int64_t AnnIndexColumnWriter::_oom_wait_timeout_ms() {
    // "fail" must not wait at all; "wait" and "skip" honor the configured timeout.
    if (config::ann_index_build_on_oom_action == "fail") {
        return 0;
    }
    return config::ann_index_build_memory_wait_timeout_ms;
}

Status AnnIndexColumnWriter::_acquire_memory_budget() {
    if (config::ann_index_build_memory_budget_bytes <= 0) {
        // Admission control disabled.
        return Status::OK();
    }
    // Initial admission reservation. The segment row count is unknown at init()
    // time, so reserve one streaming chunk's worth as a floor.
    // _ensure_reservation_for_rows() then grows the reservation toward the real
    // footprint as rows accumulate, so the global budget reflects actual memory.
    const int64_t chunk_rows =
            cast_set<int64_t>(streaming_chunk_rows(cast_set<size_t>(_build_params.dim)));
    const int64_t estimated = estimate_ann_build_memory(_build_params, chunk_rows, chunk_rows);
    const int64_t timeout_ms = _oom_wait_timeout_ms();
    _reservation = AnnBuildMemoryReservation::try_acquire(estimated, timeout_ms);
    if (_reservation.active() || estimated <= 0) {
        return Status::OK();
    }
    return _apply_oom_action(estimated, timeout_ms);
}

Status AnnIndexColumnWriter::_ensure_reservation_for_rows(int64_t total_rows) {
    if (config::ann_index_build_memory_budget_bytes <= 0) {
        return Status::OK();
    }
    // Training-free indexes stream into the index, so the input buffer peaks at
    // one chunk; indexes that need training keep the whole segment buffered.
    const int64_t chunk_rows =
            cast_set<int64_t>(streaming_chunk_rows(cast_set<size_t>(_build_params.dim)));
    const bool streaming = _vector_index->get_min_train_rows() <= 0;
    const int64_t buffered_rows = streaming ? std::min(total_rows, chunk_rows) : total_rows;
    const int64_t target = estimate_ann_build_memory(_build_params, total_rows, buffered_rows);
    const int64_t held = _reservation.bytes();
    if (target <= held) {
        return Status::OK();
    }
    const int64_t timeout_ms = _oom_wait_timeout_ms();
    if (_reservation.grow(target - held, timeout_ms)) {
        return Status::OK();
    }
    // Backpressure: the build cannot grow within the budget. "skip" discards the
    // partially built index at finish() (segment write still succeeds); "wait"/
    // "fail" abort the build with a diagnostic error.
    return _apply_oom_action(target, timeout_ms);
}

Status AnnIndexColumnWriter::_apply_oom_action(int64_t estimated_bytes, int64_t waited_ms) {
    const std::string action = config::ann_index_build_on_oom_action;
    const int64_t budget = config::ann_index_build_memory_budget_bytes;
    const int64_t in_use = AnnBuildMemoryBudget::instance().reserved_bytes();
    if (action == "skip") {
        LOG_WARNING(
                "Skipping ANN index {} build due to memory budget: estimated={} bytes, "
                "in_use={} bytes, budget={} bytes, waited={} ms",
                _index_meta->index_id(), estimated_bytes, in_use, budget, waited_ms);
        _skip_due_to_oom = true;
        return Status::OK();
    }
    // "wait" already exhausted its timeout inside try_reserve; treat as failure.
    return Status::RuntimeError(
            "ANN index {} build failed due to memory budget (action={}): "
            "estimated={} bytes, in_use={} bytes, budget={} bytes, waited={} ms",
            _index_meta->index_id(), action, estimated_bytes, in_use, budget, waited_ms);
}

Status AnnIndexColumnWriter::add_values(const std::string fn, const void* values, size_t count) {
    return Status::OK();
}

void AnnIndexColumnWriter::close_on_error() {
    _release_buffer();
    _reservation.release();
}

void AnnIndexColumnWriter::_release_buffer() {
    PODArray<float> empty_buffered_vectors;
    _buffered_vectors.swap(empty_buffered_vectors);
}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                              const uint8_t* null_map, const uint8_t* offsets_ptr,
                                              size_t num_rows) {
    // TODO: Performance optimization
    if (num_rows == 0) {
        return Status::OK();
    }
    if (_skip_due_to_oom) {
        // Admission control chose to skip this index build; drop the rows
        // silently so the surrounding segment write still succeeds.
        return Status::OK();
    }

    const auto* offsets = reinterpret_cast<const size_t*>(offsets_ptr);
    const size_t dim = _vector_index->get_dimension();
    for (size_t i = 0; i < num_rows; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        if (array_elem_size != dim) {
            return Status::InvalidArgument("Ann index expect array with {} dim, got {}.", dim,
                                           array_elem_size);
        }
    }

    const float* p = reinterpret_cast<const float*>(value_ptr);

    // The offsets check above guarantees every array row matches the ANN index dimension.
    DCHECK(p != nullptr);
    _buffered_vectors.insert(_buffered_vectors.end(), p, p + num_rows * dim);
    _total_rows += cast_set<int64_t>(num_rows);

    RETURN_IF_ERROR(_ensure_reservation_for_rows(_total_rows));
    if (_skip_due_to_oom) {
        // Backpressure chose to skip mid-build: drop the buffered rows and stop
        // accepting more. finish() deletes the index entry.
        _release_buffer();
        return Status::OK();
    }

    return _flush_streaming_rows(/*force=*/false);
}

Status AnnIndexColumnWriter::_flush_streaming_rows(bool force) {
    // Streaming add applies only to indexes that need no training (e.g. HNSW
    // flat): indexes with min_train_rows > 0 must keep the whole segment
    // buffered so train() sees all rows at once.
    if (_vector_index->get_min_train_rows() > 0) {
        return Status::OK();
    }
    // Below the persistence threshold the rows stay buffered: finish() may
    // still decide to skip this segment entirely, and building a graph that is
    // deleted right after would waste CPU.
    if (_total_rows < config::ann_index_build_min_segment_rows) {
        return Status::OK();
    }
    const size_t dim = _vector_index->get_dimension();
    if (dim == 0) {
        return Status::OK();
    }
    const size_t buffered_rows = _buffered_vectors.size() / dim;
    if (buffered_rows == 0) {
        return Status::OK();
    }
    const size_t chunk_rows = streaming_chunk_rows(dim);
    if (!force && buffered_rows < chunk_rows) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_vector_index->add(cast_set<Int64>(buffered_rows), _buffered_vectors.data()));
    _indexed_rows += cast_set<int64_t>(buffered_rows);
    _buffered_vectors.clear();
    // clear() keeps capacity for reuse; release it when an oversized incoming
    // batch ballooned the buffer past twice the chunk target.
    if (_buffered_vectors.capacity() > chunk_rows * dim * 2) {
        _release_buffer();
    }
    return Status::OK();
}

Status AnnIndexColumnWriter::add_nulls(uint32_t count) {
    return Status::InternalError("Ann index should not be used on nullable column");
}

Status AnnIndexColumnWriter::add_array_nulls(const uint8_t* null_map, size_t row_id) {
    return Status::InternalError("Ann index should not be used on nullable column");
}

int64_t AnnIndexColumnWriter::size() const {
    return 0;
}

Status AnnIndexColumnWriter::finish() {
    if (_skip_due_to_oom) {
        _release_buffer();
        return _index_file_writer->delete_index(_index_meta);
    }
    if (_total_rows == 0) {
        LOG_INFO("No data to train/add for ANN index. Skipping index building.");
        return _index_file_writer->delete_index(_index_meta);
    }

    const Int64 min_train_rows = _vector_index->get_min_train_rows();
    const Int64 effective_min_rows =
            std::max(min_train_rows, cast_set<Int64>(config::ann_index_build_min_segment_rows));
    if (_total_rows < effective_min_rows) {
        LOG_INFO(
                "Total data size {} is less than minimum {} rows required for ANN index build. "
                "Skipping index building for this segment.",
                _total_rows, effective_min_rows);
        _release_buffer();
        return _index_file_writer->delete_index(_index_meta);
    }

    if (min_train_rows <= 0) {
        // Training-free path: stream any remainder into the index and save.
        RETURN_IF_ERROR(_flush_streaming_rows(/*force=*/true));
        _release_buffer();
        return _vector_index->save(_dir.get());
    }

    return _build_and_save(min_train_rows, effective_min_rows);
}

Status AnnIndexColumnWriter::_build_and_save(Int64 min_train_rows, Int64 effective_min_rows) {
    const size_t dim = _vector_index->get_dimension();
    DCHECK(_buffered_vectors.size() % dim == 0);
    DCHECK(_indexed_rows == 0) << "indexes that need training must not stream";
    const Int64 train_rows = cast_set<Int64>(_buffered_vectors.size() / dim);
    DORIS_CHECK(train_rows == _total_rows);
    DORIS_CHECK(train_rows >= effective_min_rows);
    RETURN_IF_ERROR(_vector_index->train(train_rows, _buffered_vectors.data()));
    RETURN_IF_ERROR(_vector_index->add(train_rows, _buffered_vectors.data()));
    // PODArray::clear() keeps the allocated capacity. Swap with an empty array so the
    // full-segment build buffer is released before saving the index.
    _release_buffer();
    return _vector_index->save(_dir.get());
}
} // namespace doris::segment_v2
