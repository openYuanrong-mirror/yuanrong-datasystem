/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

/**
 * Description: CUDA callbacks injected by a CUDA-enabled application.
 */
#ifndef DATASYSTEM_UTILS_CUDA_FUNCS_H
#define DATASYSTEM_UTILS_CUDA_FUNCS_H

#include <cstddef>

namespace datasystem {

inline constexpr int kCudaSuccess = 0;
inline constexpr int kCudaErrorHostMemoryAlreadyRegistered = 712;
inline constexpr unsigned int kCudaHostRegisterPortable = 1;

enum class DsCudaMemcpyKind : int {
    HOST_TO_DEVICE = 0,
    DEVICE_TO_HOST = 1,
};

using HostRegisterFunc = int (*)(void *pointer, size_t size, unsigned int flags);
using HostUnregisterFunc = int (*)(void *pointer);
using GetErrorStringFunc = const char *(*)(int error);
using MemcpyAsyncFunc = int (*)(void *dst, const void *src, size_t size, DsCudaMemcpyKind kind, void *stream);

struct CudaFuncs {
    HostRegisterFunc hostRegister = nullptr;
    HostUnregisterFunc hostUnregister = nullptr;
    GetErrorStringFunc getErrorString = nullptr;
    MemcpyAsyncFunc memcpyAsync = nullptr;
};

}  // namespace datasystem
#endif  // DATASYSTEM_UTILS_CUDA_FUNCS_H
