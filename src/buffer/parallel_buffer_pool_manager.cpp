//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) 
                                                     :num_instances_(num_instances),
                                                      buffer_pool_managers_(),
                                                      next_buffer_index_(0){
  // Allocate and create individual BufferPoolManagerInstances
  buffer_pool_managers_.reserve(num_instances_);
  for (size_t i = 0; i < num_instances_; i++) {
    buffer_pool_managers_.emplace_back(
      new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  size_t pool_size = 0;
  for (auto &ptr : buffer_pool_managers_) {
    pool_size += ptr->GetPoolSize();
  }
  return pool_size;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  BufferPoolManagerPtr& ptr = buffer_pool_managers_[page_id % num_instances_];
  return ptr.get();
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* manager = GetBufferPoolManager(page_id);
  Page *page = manager->FetchPage(page_id);
  return page;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* manager = GetBufferPoolManager(page_id);
  return manager->UnpinPage(page_id,is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* manager = GetBufferPoolManager(page_id);
  return manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  size_t start_pos = next_buffer_index_;
  while (true) {
    BufferPoolManagerPtr& rptr = buffer_pool_managers_[next_buffer_index_];
    Page *page = rptr->NewPage(page_id);
    next_buffer_index_ = ((next_buffer_index_ + 1) % num_instances_);
    if (page != nullptr) {
      return page;
    }
    if (next_buffer_index_ == start_pos) {
      break;
    }
  }
  

  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* manager = GetBufferPoolManager(page_id);
  return manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
}

}  // namespace bustub
