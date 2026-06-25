cmake_minimum_required(VERSION 3.16)
project(picam_frontend CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release CACHE STRING "")

add_executable(picam_frontend src/main.cpp)

target_include_directories(picam_frontend PRIVATE
        src
)

target_link_libraries(picam_frontend PRIVATE
    pthread
)

target_compile_options(picam_frontend PRIVATE
    -O3 -Wall -Wextra -pthread
)

install(TARGETS picam_frontend DESTINATION /usr/bin)
install(DIRECTORY web DESTINATION /usr/share/picam-frontend/web)
