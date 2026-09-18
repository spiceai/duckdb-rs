#include "thrift/Thrift.h"

#include <cassert>
#include <map>
#include <string>

using duckdb_apache::thrift::TEnumIterator;

static void check_end(TEnumIterator &iterator, const TEnumIterator &end, bool exhausted) {
  assert((iterator == end) == exhausted);
  assert((iterator != end) == !exhausted);
}

int main() {
  const TEnumIterator end(-1, nullptr, nullptr);
  TEnumIterator empty(0, nullptr, nullptr);
  check_end(empty, end, true);
  assert((std::map<int, const char *>(empty, end).empty()));

  int values[] = {3, -1, 42};
  const char *names[] = {"three", "minus one", "forty-two"};
  for (int count : {1, 3}) {
    TEnumIterator iterator(count, values, names);
    for (int i = 0; i < count; ++i) {
      check_end(iterator, end, false);
      const auto entry = *iterator;
      assert(entry.first == values[i]);
      assert(std::string(entry.second) == names[i]);
      ++iterator;
    }
    check_end(iterator, end, true);

    // This is the range constructor used by parquet_types.cpp. Newer libc++
    // implementations compare the iterators with == as well as !=.
    const std::map<int, const char *> entries(TEnumIterator(count, values, names), end);
    assert(entries.size() == static_cast<std::size_t>(count));
    for (int i = 0; i < count; ++i) {
      assert(std::string(entries.at(values[i])) == names[i]);
    }
  }
}
