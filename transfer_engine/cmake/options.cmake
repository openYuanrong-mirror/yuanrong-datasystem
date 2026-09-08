option(TRANSFER_ENGINE_ENABLE_HIXL "Enable Ascend backend backed by HIXL" ON)
option(TRANSFER_ENGINE_BUILD_TESTS "Build transfer_engine tests" ON)
option(TRANSFER_ENGINE_BUILD_PYTHON "Build transfer_engine python bindings" OFF)

set(TRANSFER_ENGINE_PYTHON_OUTPUT_DIR "" CACHE PATH "Output directory of python extension module")
