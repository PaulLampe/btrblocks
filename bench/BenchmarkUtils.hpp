#pragma once
#include "btrblocks.hpp"

namespace btrbench {
using namespace btrblocks;

static void SetupAllSchemes() {
  BtrBlocksConfig::get().integers.schemes = defaultIntegerSchemes();
  BtrBlocksConfig::get().doubles.schemes = defaultDoubleSchemes();
  BtrBlocksConfig::get().strings.schemes = defaultStringSchemes();

  SchemePool::refresh();
}

static void SetupSchemes(const vector<IntegerSchemeType>& schemes) {
  BtrBlocksConfig::get().integers.schemes = SchemeSet<IntegerSchemeType>({});
  BtrBlocksConfig::get().integers.schemes.enable(IntegerSchemeType::UNCOMPRESSED);
  BtrBlocksConfig::get().integers.schemes.enable(IntegerSchemeType::ONE_VALUE);
  for (auto& scheme : schemes) {
    BtrBlocksConfig::get().integers.schemes.enable(scheme);
  }

  SchemePool::refresh();
}
}