#pragma once

#define APP_VERSION_MAJOR 1
#define APP_VERSION_MINOR 0
#define APP_VERSION_PATCH 0
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

#define APP_VERSION                                                            \
  TOSTRING(APP_VERSION_MAJOR)                                                  \
  "." TOSTRING(APP_VERSION_MINOR) "." TOSTRING(APP_VERSION_PATCH)
