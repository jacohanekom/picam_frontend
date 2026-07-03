cmake_minimum_required(VERSION 3.16)
project(picam_frontend CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release CACHE STRING "")

# libdatachannel isn't packaged for Debian — vendor it via FetchContent,
# pinned to a released tag. Static + trimmed to the media path only
# (we never use WebSocket or the example/test targets); needs network
# access at configure time (see README's Build section for the tradeoff).
# This is the first media/codec-adjacent dependency this project has
# ever had — picam-frontend relays raw RTP bytes (no decode/encode), so
# it needs libdatachannel but not libvpx.
include(FetchContent)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(NO_WEBSOCKET ON CACHE BOOL "" FORCE)
set(NO_EXAMPLES ON CACHE BOOL "" FORCE)
set(NO_TESTS ON CACHE BOOL "" FORCE)
FetchContent_Declare(datachannel
    GIT_REPOSITORY https://github.com/paullouisageneau/libdatachannel.git
    GIT_TAG        v0.24.5
    GIT_SUBMODULES_RECURSE TRUE
)
FetchContent_MakeAvailable(datachannel)

add_executable(picam_frontend src/main.cpp)

target_include_directories(picam_frontend PRIVATE
        src
)

target_link_libraries(picam_frontend PRIVATE
    datachannel
    pthread
)

target_compile_options(picam_frontend PRIVATE
    -O3 -Wall -Wextra -pthread
)

install(TARGETS picam_frontend DESTINATION /usr/bin)
# Trailing slash on the source matters here: "web" (no slash) would
# install the directory itself under DESTINATION, landing at
# .../web/web/index.html instead of .../web/index.html.
install(DIRECTORY web/ DESTINATION /usr/share/picam-frontend/web)
