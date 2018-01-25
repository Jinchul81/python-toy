#include <fstream>
#include <limits>
#include <iostream>
#include <string>
#include <sys/sysinfo.h>
#include <thread>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <cassert>
#include <string.h>
#include <thread>
#include <chrono>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <boost/pool/poolfwd.hpp>
#include <boost/pool/pool.hpp>
#include <boost/pool/pool_alloc.hpp>

#define BOOST_FILESYSTEM_VERSION 3
#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

namespace fs = ::boost::filesystem;

using namespace std;

#define UNEXPECTE_ERROR assert(false)
//#define _DEBUG_NO_INLINE_
#ifdef _DEBUG_NO_INLINE_
#  define _DEBUG_NO_INLINE_ATTRIBUTE_ __attribute__ ((noinline))
#else
#  define _DEBUG_NO_INLINE_ATTRIBUTE_
#endif // _DEBUG_NO_INLINE_
//#define _DEBUG_MODE_

// Forward declarations
class Element;
class Record;
class AbstractThread;
class Dictionary;
class ConcurrentCharStringChainMap;
class GlobalContext;

typedef std::chrono::system_clock::time_point TimePoint;
typedef std::chrono::duration<double> Seconds;
typedef vector<char> CharVector;
typedef vector<char*> CharPtrVector;
typedef vector<size_t> SizeVector;
typedef shared_ptr<unsigned long> UnsignedLongPtr;
typedef shared_ptr<string> StringPtr;
typedef vector<string> StringVector;
typedef vector<StringVector > StringVectorVector;
typedef vector<StringPtr> StringPtrVector;
typedef shared_ptr<StringPtrVector> StringPtrVectorPtr;
typedef AbstractThread* AbstractThreadPtr;
typedef vector<AbstractThreadPtr> AbstractThreadPtrVector;
typedef shared_ptr<thread> ThreadPtr;
typedef vector<ThreadPtr> ThreadPtrVector;
typedef shared_ptr<Record> RecordPtr;
typedef shared_ptr<Element> ElementPtr;
typedef vector<RecordPtr> RecordPtrVector;
typedef shared_ptr<RecordPtrVector> RecordPtrVectorPtr;
typedef Record& RecordRef;
typedef unordered_map<size_t, size_t> SizeMap;
typedef vector<fs::path> PathVector;
typedef unordered_map<string, StringVector> AliasMap;
typedef shared_ptr<StringVector > StringVectorPtr;
typedef shared_ptr<Dictionary> DictionaryPtr;
// TODO: supports boost's pool allocator
//typedef vector<shared_ptr<string>, boost::pool_allocator<string> > StringVector;
//typedef boost::fast_pool_allocator<char> c_allocator;
//typedef basic_string<char, char_traits<char>, c_allocator> String;

static const char CHAR_ZERO = '0';
static const string STRING_ZERO = "0";

template <typename T>
class ConcurrentQueue
{
public:
  ConcurrentQueue()
    : size_(0)
  {
  }
  ConcurrentQueue(const ConcurrentQueue&) = delete;
  ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;

  T pop()
  {
    unique_lock<mutex> mlock(mutex_);
    if (0 == size_)
    {
      return nullptr;
    }
    auto val = queue_.front();
    queue_.pop();
    --size_;
    return val;
  }

  T waitAndPop()
  {
    unique_lock<mutex> mlock(mutex_);
    while (queue_.empty())
    {
      cond_.wait(mlock);
    }
    auto val = queue_.front();
    queue_.pop();
    --size_;
    return val;
  }

  bool empty()
  {
    unique_lock<mutex> mlock(mutex_);
    return 0 == size_;
  }

  size_t size()
  {
    unique_lock<mutex> mlock(mutex_);
    return size_;
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toString()
  {
    stringstream ss;
    unique_lock<mutex> mlock(mutex_);
    ss << "{size: " << queue_.size() << "}";
    return ss.str();
  }

  void push(const T& item)
  {
    unique_lock<mutex> mlock(mutex_);
    queue_.push(item);
    ++size_;
    mlock.unlock();
    cond_.notify_one();
  }

private:
  queue<T> queue_;
  mutex mutex_;
  size_t size_;
  condition_variable cond_;
};


typedef ConcurrentQueue<StringPtrVectorPtr> StringPtrVectorPtrConcurrentQueue;
typedef ConcurrentQueue<StringVectorPtr> StringVectorPtrConcurrentQueue;
typedef ConcurrentQueue<RecordPtrVector> RecordPtrVectorConcurrentQueue;
typedef ConcurrentQueue<RecordPtrVectorPtr> RecordPtrVectorPtrConcurrentQueue;
typedef shared_ptr<ConcurrentCharStringChainMap> ConcurrentCharStringChainMapPtr;
typedef ConcurrentQueue<ConcurrentCharStringChainMapPtr> ConcurrentCharStringChainMapPtrConcurrentQueue;
typedef shared_ptr<RecordPtrVectorPtrConcurrentQueue> RecordPtrVectorPtrConcurrentQueuePtr;
typedef shared_ptr<RecordPtrVectorConcurrentQueue> RecordPtrVectorConcurrentQueuePtr;
typedef vector<ConcurrentCharStringChainMap> ConcurrentCharStringChainMapVector;
typedef vector<ConcurrentCharStringChainMapPtr> ConcurrentCharStringChainMapPtrVector;
typedef shared_ptr<ConcurrentCharStringChainMapPtrConcurrentQueue> ConcurrentCharStringChainMapPtrConcurrentQueuePtr;

static shared_ptr<string> globalEmptyString(new string());

namespace utility {
  static size_t getNumCores()
  {
    return thread::hardware_concurrency();
  }

  static TimePoint getCurrentTime()
  {
    return std::chrono::system_clock::now();
  }

  static size_t convertToUnicodeSize(const size_t s)
  {
    const size_t resized = 3*s;
    assert(resized > s);
    return resized;
  }

  static void safeStrncpy(char* dest, const char* src, size_t size)
  {
    assert(dest && src);
    strncpy(dest, src, size);
    dest += size;
    *dest = '\0';
  }
/*
  static bool existsFile(const string& path)
  {
    const fs::path filepath(path);
    return fs::exists(filepath);
  }
*/
  static bool isDirectory(const string& path)
  {
    const fs::path directoryPath(path);
    return fs::exists(directoryPath) && fs::is_directory(directoryPath);
  }

  static void getFilePaths(const string& targetDirectory,
                           const string& pattern,
                           PathVector& pathVector)
  {
    const fs::path directoryPath(targetDirectory);
    if (!fs::exists(directoryPath) || !fs::is_directory(directoryPath)) return;

    fs::recursive_directory_iterator it(directoryPath);
    const fs::recursive_directory_iterator endIt;

    for (; endIt != it; ++it)
    {
      if (fs::is_regular_file(*it) && 0 == it->path().filename().string().find(pattern))
      {
        pathVector.push_back(it->path());
      }
    }
  }

  namespace regex {
    inline static
    bool getRegexMatchedString(string* dest
                             , const char* src
                             , const char* pattern
                             , size_t index)
    {
      boost::regex e(pattern);
      boost::smatch sm;
      bool matched = boost::regex_match(string(src), sm, e);
      if (matched)
      {
        assert(index < sm.size());
        if (dest)
        {
          *dest = sm[index];
        }
      }
      return matched;
    }

    inline static
    bool isNumber(const char* src)
    {
      return getRegexMatchedString(nullptr, src, "([0-9]+)", 1);
    }

    inline static
    bool isNumber(const string& src)
    {
      return isNumber(src.c_str());
    }

    inline static
    bool getNumber(string& dest, const char* src)
    {
      return getRegexMatchedString(&dest, src, "([0-9]+)(.*)", 1);
    }

    inline static
    bool getHoNumber(string& dest, const char* src)
    {
      if (getRegexMatchedString(&dest, src, "([0-9]+)(호)(.*)", 1))
      {
        return true;
      }
      if (getRegexMatchedString(&dest, src, "([0-9]+)(동)([0-9]+)(호)(.*)", 3))
      {
        return true;
      }
      return false;
    }

    inline static
    bool getFloorNumber(string& dest, const char* src)
    {
      return getRegexMatchedString(&dest, src, "([0-9]+)(층)(.*)", 1);
    }

    inline static
    bool getPartForDong(string& dest, const char* src)
    {
      return getRegexMatchedString(&dest, src, "([0-9]+동)(.*)", 1);
    }

    inline static
    bool getNumberForBeonji(string& dest, const char* src)
    {
      return getRegexMatchedString(&dest, src, "([0-9]+)(번지.*)", 1);
    }

    inline static
    bool getNumberForHo(string& dest, const char* src)
    {
      return getRegexMatchedString(&dest, src, "([0-9]+번지.*)([0-9]+)(호.*)", 2);
    }

    inline static
    bool getNumbersForBeonjiAndHo(StringVector& dest, const char* src)
    {
      string beonji;
      string ho;
      if (getNumberForBeonji(beonji, src))
      {
        dest.push_back(beonji);
        if (getNumberForHo(ho, src))
        {
          dest.push_back(ho);
        }

        return true;
      }
      return false;
    }

    inline static
    bool getAlternativesForAptDong(StringVector& dest, const char* src)
    {
      string s;
      dest.clear();
      if (getRegexMatchedString(&s, src, "([0-9]+)(동)(.*)", 1))
      {
        dest.push_back(s);
        dest.push_back(s+"동");
        dest.push_back(s+"호동");
        dest.push_back(string("제")+s+"동");
        dest.push_back(string("제 ")+s+"동");
        return true;
      }
      return false;
    }

    inline static
    bool getAlternativesForHangJungDong(StringVector& dest, const char*src)
    {
      string s;
      dest.clear();
      if (getRegexMatchedString(&s, src, "([가-힣]+)([0-9]+동)", 1))
      {
        dest.push_back(string(s)+"1동");
        dest.push_back(string(s)+"2동");
        dest.push_back(string(s)+"3동");
        dest.push_back(string(s)+"5동");
        dest.push_back(string(s)+"6동");
        dest.push_back(string(s)+"7동");
        dest.push_back(string(s)+"8동");
        dest.push_back(string(s)+"9동");
        dest.push_back(string(s)+"10동");
        return true;
      }
      return false;
    }

    inline static
    bool getNumberFromDetailedBuilding(string& dest, const char* src)
    {
      if (isNumber(src))
      {
        dest = src;
        return true;
      }
      if (getPartForDong(dest, src) && getNumber(dest, dest.c_str()))
      {
        return true;
      }
      if (getNumber(dest, src))
      {
        return true;
      }
      return false;
    }
  } // namespace regex

  class ScopeDuration
  {
  public:
    ScopeDuration(const string& message)
      : message_(message)
      , start_(getCurrentTime())
    {
    }

    ~ScopeDuration()
    {
      const Seconds sec = (getCurrentTime() - start_);
      cout << message_ << ": " << sec.count() << " seconds" << endl;
    }

  private:
    const string message_;
    const TimePoint start_;
  };

  template <typename T>
  class AutoCloser
  {
  public:
    AutoCloser() = default;
    ~AutoCloser()
    {
      while (! queue_.empty()) {
        T item = queue_.front();
        queue_.pop();

        if (item)
        {
          delete item;
        }
      }
    }

    void push(const T item)
    {
      assert(item);
      queue_.push(item);
    }

  private:
    queue<T> queue_;
  };

  struct StreamPadding
  {
    StreamPadding(char f, int w)
      : fill_(f), width_(w)
    {}

    char fill_;
    char width_;
  };

  struct StreamZeroPadding : public StreamPadding
  {
    StreamZeroPadding(int w)
      : StreamPadding(CHAR_ZERO, w)
    {}
  };

  ostream& operator<<(ostream& o, const StreamPadding& p)
  {
    o.fill(p.fill_);
    o.width(p.width_);
    return o;
  }
} // namespace utility

class IndentingOStreambuf : public streambuf
{
public:
  explicit IndentingOStreambuf(streambuf* dest
                             , size_t indent = 2)
    : myDest(dest)
    , myIsAtStartOfLine(true)
    , myIndent(indent, ' ')
    , myOwner(nullptr)
  {
  }

  explicit IndentingOStreambuf(ostream& dest
                             , size_t indent = 2)
    : myDest(dest.rdbuf())
    , myIsAtStartOfLine(true)
    , myIndent(indent, ' ')
    , myOwner(&dest)
  {
    myOwner->rdbuf(this);
  }

  virtual ~IndentingOStreambuf()
  {
    if (myOwner != NULL) {
      myOwner->rdbuf(myDest);
    }
  }

protected:
  virtual int overflow(int ch)
  {
    if (myIsAtStartOfLine && ch != '\n')
    {
      myDest->sputn(myIndent.data(), myIndent.size());
    }
    myIsAtStartOfLine = ch == '\n';
    return myDest->sputc(ch);
  }

private:
  streambuf* myDest;
  bool myIsAtStartOfLine;
  string myIndent;
  ostream* myOwner;
};

class Metadata
{
public:
  Metadata(const char separator)
    : separator_(separator)
  {
  }

public:
  char getSeparator() const
  {
    return separator_;
  }

private:
  const char separator_;
};

class Record : public StringPtrVector
{
public:
  Record(const Metadata& metadata)
    : metadata_(metadata)
  {
    init();
  }

  const Metadata getMetadata() const
  {
    return metadata_;
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toString() const
  {
    stringstream ss;

    for (size_t i = 0; i < size(); ++i)
    {
      if (i != 0) {
        ss << ", ";
      }
      ss << "{" << *at(i) << "}";
    }

    return ss.str();
  }

private:
  void init()
  {
  }

private:
  const Metadata& metadata_;
};

struct KeyHashFunc
{
  size_t operator() (const char* s) const
  {
    return std::hash<string>()(s);
  }
};

struct KeyEqualFunc
{
  bool operator() (const char* lhs, const char* rhs) const
  {
    assert(nullptr != lhs && nullptr != rhs);
    if (lhs == rhs)
    {
      return true;
    }
    if (strlen(lhs) != strlen(rhs))
    {
      return false;
    }
    if (! strcmp(lhs, rhs))
    {
      return true;
    }
    return false;
  }
};

class Element
{
public:
  Element()
    : nextMap_(nullptr)
    , value_(nullptr)
  {
  }

  void clear()
  {
    nextMap_ = nullptr;
    value_ = nullptr;
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  bool isEmpty() const
  {
    return (nullptr == nextMap_.get())
        && (nullptr == value_);
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toString() const
  {
    stringstream ss;
    ss << "nextMap: "
       << (nullptr == nextMap_.get() ? "<null>" : "<not null>")
       << ", value: "
       << (nullptr == value_ ? "<null>" : value_);
    return ss.str();
  }

  ConcurrentCharStringChainMapPtr nextMap_;
  const char* value_;
};

template <typename Key,
          typename Value,
          typename HashFunc = std::hash<Key>,
          typename Comparator = std::equal_to<Key> >
class ConcurrentMap
  : protected unordered_map<Key, Value, HashFunc, Comparator>
{
public:
  typedef unordered_map<Key, Value, HashFunc, Comparator> _Base;
  typedef typename _Base::const_iterator const_iterator;
  typedef typename _Base::key_type key_type;
  typedef typename _Base::size_type size_type;
  typedef typename _Base::value_type value_type;

  _DEBUG_NO_INLINE_ATTRIBUTE_
  bool empty() const
  {
    return _Base::empty();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  const_iterator begin() const
  {
    return _Base::begin();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  const_iterator end() const
  {
    return _Base::end();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  size_type size() const
  {
    return _Base::size();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  const Value* getValuePtrOnLock(const key_type k)
  {
    unique_lock<mutex> mlock(mutex_);
    const_iterator itr = find(k);
    if (end() != itr)
    {
      return &(itr->second);
    }
    return nullptr;
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  const_iterator find(const key_type& k) const
  {
    return _Base::find(k);
  }

  const_iterator findOrGetNext(const key_type& k) const
  {
    if (size() == 1)
    {
      return begin();
    }
    return _Base::find(k);
  }

  pair<const_iterator, bool> insert(const value_type& v)
  {
    unique_lock<mutex> mlock(mutex_);
    return _Base::insert(v);
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toStringForKey() const
  {
    stringstream ss;
    if (empty())
    {
      ss << "{<empty>}";
    }
    else
    {
      StringVector stringVector;
      const_iterator itr = begin();
      for (; itr != end(); ++itr)
      {
  //      string s(*itr);
        stringVector.push_back(itr->first);
      }

      sort(stringVector.begin(), stringVector.end());
      ss << "{";
      for (auto& s : stringVector)
      {
        ss << " [" << s << "]";
      }
      ss << " }";
    }
    return ss.str();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toSimpleString(size_t indent = 2) const
  {
    return toString(indent, false);
  }

  virtual string toString(size_t indent = 2, bool isDetail = true) const = 0;
  virtual string toStringForInternalNodes(size_t indent = 2) const = 0;

protected:
  mutex mutex_;
};

class ConcurrentCharStringChainMap
  : public ConcurrentMap<const char*, Element, KeyHashFunc, KeyEqualFunc>
{
public:
  _DEBUG_NO_INLINE_ATTRIBUTE_
  virtual string toString(size_t indent = 2, bool isDetail = true) const
  {
    stringstream ss;
    IndentingOStreambuf ios(ss, indent);
    const size_t level = indent/2;
    for (const_iterator itr = begin(); itr != end(); ++itr)
    {
      ss << "[L" << level << "]key: [" << itr->first << "]";
      if (itr->second.value_)
      {
        if (isDetail)
        {
          ss << " value: [" << itr->second.value_ << "]";
        }
      }
      else
      {
        ss << itr->second.nextMap_->toString(indent+2);
      }
    }
    ss << endl;

    return ss.str();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  virtual string toStringForInternalNodes(size_t indent = 2) const
  {
    stringstream ss;
    IndentingOStreambuf ios(ss, indent);
    ss << size();
    for (const_iterator itr = begin(); itr != end(); ++itr)
    {
      if (! itr->second.value_)
      {
        ss << itr->second.nextMap_->toStringForInternalNodes(indent+2);
      }
    }
    ss << endl;

    return ss.str();
  }
};
typedef pair<ConcurrentCharStringChainMap::const_iterator, bool> ConcurrentCharStringChainMapPair;
typedef const pair<ConcurrentCharStringChainMap::const_iterator, bool> ConstConcurrentCharStringChainMapPair;

class BuildingKeyMap
  : public ConcurrentMap<const char*, RecordPtr, KeyHashFunc, KeyEqualFunc>
{
  _DEBUG_NO_INLINE_ATTRIBUTE_
  virtual string toString(size_t indent = 2, bool isDetail = true) const
  {
    stringstream ss;
    IndentingOStreambuf ios(ss, indent);
    const size_t level = indent/2;
    for (const_iterator itr = begin(); itr != end(); ++itr)
    {
      ss << "[L" << level << "]key: [" << itr->first << "]";
      ss << " value: [" << itr->second->toString() << "]";
      ss << endl;
    }

    return ss.str();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  virtual string toStringForInternalNodes(size_t indent = 2) const
  {
    // TODO
    string s;
    return s;
  }
};

class FileNotFoundException : public runtime_error
{
public:
  FileNotFoundException(string const& message)
    : runtime_error("File not found: " + message)
  {
  }
};

class FileCreateException : public runtime_error
{
public:
  FileCreateException(string const& message)
    : runtime_error("File cannot be created: " + message)
  {
  }
};

static const size_t MINIMUM_NUM_CORES = 2;

// Only used for loading juso file data
enum class IndexesOfBuilding {
  CODE_B_DONG             = 0  /* 1  법정동 코드 */,
  NAME_SI_DO              = 1  /* 2  시도명 */,
  NAME_SI_GUN_GU          = 2  /* 3  시군구명 => {1:시, 2:군/구} */,
  NAME_B_E_M_DONG         = 3  /* 4  법정읍면동명 */,
  NAME_B_L                = 4  /* 5  법정리명 */,
  FLAG_SAN                = 5  /* 6  산여부 => {0:대지, 1:산} */,
  CODE_BUNJI1             = 6  /* 7  지번본번(번지) */,
  CODE_BUNJI2             = 7  /* 8  지번부번(호) */,
  CODE_ROAD               = 8  /* 9  도로명코드 = 시군구코드(5) + 도로명번호(7) */,
  NAME_ROAD               = 9  /* 10 도로명 */,
  FLAG_BASEMENT           = 10 /* 11 지하여부 => {0:지상, 1:지하, 2:공중} */,
  CODE_BUILDING1          = 11 /* 12 건물본번 */,
  CODE_BUILDING2          = 12 /* 13 건물부번 */,
  NAME_BUILDING           = 13 /* 14 건축물대장 건물명 */,
  NAME_DETAILED_BD        = 14 /* 15 상세건물명 */,
  CODE_BUILDING_PK        = 15 /* 16 건물관리번호 (PK) */,
  CODE_SEQ_E_M_DONG       = 16 /* 17 읍면동일련번호 */,
  CODE_DONG               = 17 /* 18 행정동코드 */,
  NAME_DONG               = 18 /* 19 행정동명 */
};

enum class IndexesOfRecord {
  CODE_B_DONG             = 0  /* 0  법정동 코드 */,
  NAME_SI_DO              = 1  /* 1  시도명 */,
  NAME_SI                 = 2  /* 2  시 */,
  NAME_GUN_GU             = 3  /* 2  군구명 */,
  NAME_B_E_M_DONG         = 4  /* 3  법정읍면동명 */,
  NAME_B_L                = 5  /* 4  법정리명 */,
  FLAG_SAN                = 6  /* 5  산여부 => {0:대지, 1:산} */,
  CODE_BUNJI1             = 7  /* 6  지번본번(번지) */,
  CODE_BUNJI2             = 8  /* 7  지번부번(호) */,
  CODE_ROAD               = 9  /* 8  도로명코드 = 시군구코드(5) + 도로명번호(7) */,
  NAME_ROAD               = 10 /* 9  도로명 */,
  FLAG_BASEMENT           = 11 /* 10 지하여부 => {0:지상, 1:지하, 2:공중} */,
  CODE_BUILDING1          = 12 /* 11 건물본번 */,
  CODE_BUILDING2          = 13 /* 12 건물부번 */,
  NAME_BUILDING           = 14 /* 13 건축물대장 건물명 */,
  NAME_DETAILED_BD        = 15 /* 14 상세건물명 */,
  CODE_BUILDING_PK        = 16 /* 15 건물관리번호 (PK) */,
  CODE_SEQ_E_M_DONG       = 17 /* 16 읍면동일련번호 */,
  CODE_DONG               = 18 /* 17 행정동코드 */,
  NAME_DONG               = 19 /* 18 행정동명 */
};

size_t CAST(IndexesOfBuilding i)
{
  return static_cast<size_t>(i);
}

size_t CAST(IndexesOfRecord i)
{
  return static_cast<size_t>(i);
}

static const char CHAR_SINGLE_SPACE = ' ';

static const char SEPARATOR_FOR_INPUT = 0x01;
static const char SEPARATOR_FOR_OR = '|';
static const char SEPARATOR_FOR_ADDR_PART1 = CHAR_SINGLE_SPACE;
static const char SEPARATOR_FOR_ADDR_PART2 = CHAR_SINGLE_SPACE;
static const char SEPARATOR_FOR_ALIAS = CHAR_SINGLE_SPACE;

static const size_t INDEX_ADDR_KEY             = 0;
static const size_t INDEX_ADDR_PART1_FOR_INPUT = 1;
static const size_t INDEX_ADDR_PART2_FOR_INPUT = 2;

static Metadata metadataForNewAddr(SEPARATOR_FOR_OR);
static Metadata metadataForInput(SEPARATOR_FOR_INPUT);
static Metadata metadataForAddrPart1(SEPARATOR_FOR_ADDR_PART1);
static Metadata metadataForAddrPart2(SEPARATOR_FOR_ADDR_PART2);

class GlobalContext
{
public:
  GlobalContext(
      const string& sourceDirectory,
      const string& inputDirectory,
      const string& filepathForSuccesses,
      const string& filepathForFailures,
      const size_t initialBucketSize,
      const size_t keyIndex)
    : sourceDirectory_(sourceDirectory)
    , inputDirectory_(inputDirectory)
    , filepathForSuccesses_(filepathForSuccesses)
    , filepathForFailures_(filepathForFailures)
    , initialBucketSize_(initialBucketSize)
    , keyIndex_(keyIndex)
  {
  }

  const string& getSourceDirectory() const
  {
    return sourceDirectory_;
  }

  const string& getInputDirectory() const
  {
    return inputDirectory_;
  }

  const string& getFilenameForSuccesses() const
  {
    return filepathForSuccesses_;
  }

  const string& getFilenameForFailures() const
  {
    return filepathForFailures_;
  }

  size_t getInitialBucketSize() const
  {
    return initialBucketSize_;
  }

  size_t getKeyIndex() const
  {
    return keyIndex_;
  }

private:
  const string sourceDirectory_;
  const string inputDirectory_;
  const string filepathForSuccesses_;
  const string filepathForFailures_;
  const size_t initialBucketSize_;
  const size_t keyIndex_;
};

class LineSplitter
{
public:
  static void parse(const char* line
                  , StringVector& dest
                  , const char separator)
  {
    assert(line);

    const char* prevPos = line;
    const char* currPos = prevPos;
    const size_t bufSize = 1024;
    char buf[bufSize];
    dest.clear();
    for (; '\0' != *currPos; currPos++)
    {
      if (separator == *currPos)
      {
        if (prevPos == currPos)
        {
          *buf = '\0';
        }
        else
        {
          const size_t length = currPos - prevPos;
          assert(length < bufSize);
          utility::safeStrncpy(buf, prevPos, length);
          dest.push_back(buf);
        }
        prevPos = currPos+1;
      }
    }

    if (*(currPos-1) == separator)
    {
      *buf = '\0';
    }
    else
    {
      const size_t length = currPos - prevPos;
      assert(length < bufSize);
      utility::safeStrncpy(buf, prevPos, length);
      dest.push_back(buf);
    }
  }

  static void parse(const char* line
                  , RecordRef recordRef)
  {
    const SizeVector requiredIndexes;
    parse(line, recordRef, requiredIndexes);
  }

  static void parseForSourceAddr(const char* line
                               , RecordRef recordRef
                               , const SizeVector& requiredIndexes)
  {
    assert(line);

    recordRef.clear();
    const char* prevPos = line;
    const char* currPos = prevPos;
    size_t idx = 0;
    const Metadata& metadata = recordRef.getMetadata();
    const char separator = metadata.getSeparator();
    const size_t bufSize = 512;
    char buf[bufSize];
    for (; '\0' != *currPos; currPos++)
    {
      if (separator == *currPos)
      {
        if (requiredIndexes.empty()
              || find(requiredIndexes.begin(), requiredIndexes.end(), idx) != requiredIndexes.end())
        {
          const size_t length = currPos - prevPos;
          assert(length < bufSize);
          utility::safeStrncpy(buf, prevPos, length);
          shared_ptr<string> s(new string(buf));
          recordRef.push_back(s);
        }
        prevPos = currPos+1;
        idx++;
      }
    }

    if (*(currPos-1) == separator)
    {
      recordRef.push_back(globalEmptyString);
    }
    else
    {
      if (requiredIndexes.empty()
          || find(requiredIndexes.begin(), requiredIndexes.end(), idx) != requiredIndexes.end())
      {
        const size_t length = currPos - prevPos;
        assert(length < bufSize);
        utility::safeStrncpy(buf, prevPos, length);
        shared_ptr<string> s(new string(buf));
        recordRef.push_back(s);
      }
    }
    idx++;
  }

  static void parse(const char* line
                  , RecordRef recordRef
                  , const SizeVector& requiredIndexes)
  {
    assert(line);

    recordRef.clear();
    const char* prevPos = line;
    const char* currPos = prevPos;
    size_t idx = 0;
    const Metadata& metadata = recordRef.getMetadata();
    const char separator = metadata.getSeparator();
    const size_t bufSize = 512;
    char buf[bufSize];
    for (; '\0' != *currPos; currPos++)
    {
      if (separator == *currPos)
      {
        if (requiredIndexes.empty()
              || find(requiredIndexes.begin(), requiredIndexes.end(), idx) != requiredIndexes.end())
        {
          const size_t length = currPos - prevPos;
          assert(length < bufSize);
          utility::safeStrncpy(buf, prevPos, length);
          shared_ptr<string> s(new string(buf));
          recordRef.push_back(s);
        }
        prevPos = currPos+1;
        idx++;
      }
    }

    if (*(currPos-1) == separator)
    {
      recordRef.push_back(globalEmptyString);
    }
    else
    {
      if (requiredIndexes.empty()
          || find(requiredIndexes.begin(), requiredIndexes.end(), idx) != requiredIndexes.end())
      {
        const size_t length = currPos - prevPos;
        assert(length < bufSize);
        utility::safeStrncpy(buf, prevPos, length);
        shared_ptr<string> s(new string(buf));
        recordRef.push_back(s);
      }
    }
    idx++;
  }
};

class AbstractThread
{
public:
  AbstractThread(size_t id)
   : terminate_(false)
   , id_(id)
  {
  }

  virtual ~AbstractThread()
  {
  }

  virtual void run() const = 0;

  void terminate()
  {
    terminate_ = true;
  }

  size_t getId() const
  {
    return id_;
  }

protected:
  bool terminate_;
  size_t id_;
  unsigned MILLISECONDS_FOR_SLEEP = 10;
};

class Dictionary
{
public:
  Dictionary()
    : concurrentRecordMapPtrConcurrentQueue_()
    , primaryMapPtr_(new ConcurrentCharStringChainMap())
    , secondaryMapPtr_(new ConcurrentCharStringChainMap())
    , buildingKeyMap_()
    , aliasMap_()
  {
    buildAliasMap();
  }

  void insert(const RecordPtr& recordPtr)
  {
    static const size_t indexOfValue = CAST(IndexesOfRecord::CODE_BUILDING_PK);

    buildingKeyMap_.insert(make_pair(recordPtr->at(indexOfValue).get()->c_str(), recordPtr));

    insertNewAddr1(recordPtr, indexOfValue);
    insertNewAddr2(recordPtr, indexOfValue);
    insertOldAddr1(recordPtr, indexOfValue);
    insertOldAddr2(recordPtr, indexOfValue);
  }

  void buildSecondaryMap() const
  {
    const utility::ScopeDuration prepareDuration("* Elapsed time for buildSecondaryMap ");
#ifdef _DEBUG_MODE_
    stringstream err;
#endif // _DEBUG_MODE_
    ConcurrentCharStringChainMap::const_iterator itr = primaryMapPtr_->begin();
    for (; itr != primaryMapPtr_->end(); ++itr)
    {
#ifdef _DEBUG_MODE_
      err << itr->first << endl;
#endif // _DEBUG_MODE_
      ConcurrentCharStringChainMapPtr mapPtr = itr->second.nextMap_;
      assert(mapPtr);

      ConcurrentCharStringChainMap::const_iterator mItr = mapPtr->begin();
      for (; mItr != mapPtr->end(); ++mItr)
      {
#ifdef _DEBUG_MODE_
        IndentingOStreambuf ios(err);
        err << mItr->first << endl;
#endif // _DEBUG_MODE_
        ConcurrentCharStringChainMapPtr mapMapPtr = mItr->second.nextMap_;
        if (strlen(mItr->first) == 0)
        {
          ConcurrentCharStringChainMap::const_iterator mmItr = mapMapPtr->begin();
          for (; mmItr != mapMapPtr->end(); ++mmItr)
          {
#ifdef _DEBUG_MODE_
            IndentingOStreambuf ios(err);
            err << mItr->first << endl;
#endif // _DEBUG_MODE_
            if (strlen(mmItr->first) != 0)
            {
              secondaryMapPtr_->insert(make_pair(mmItr->first, mmItr->second));
            }
          }
        }
        else
        {
          secondaryMapPtr_->insert(make_pair(mItr->first, mItr->second));
        }
      }
    }
#ifdef _DEBUG_MODE_
    cout << "buildSecondaryMap" << endl << err.str() << endl;
#endif // _DEBUG_MODE_
  }

  bool search(const Record& recordForAddrPart1, const Record& recordForAddrPart2, string& dmapKey) const
  {
    ConcurrentCharStringChainMapPtr currentRecordMapPtr(primaryMapPtr_);
    ConcurrentCharStringChainMapPtrVector mapHistory;
    stringstream err;
#ifdef _DEBUG_MODE_
    err << "input addr: part1: " << recordForAddrPart1.toString() << ", part2: " << recordForAddrPart2.toString() << endl;
#endif // _DEBUG_MODE_
    const char* keyOfBuilding = searchInAddrPart1(currentRecordMapPtr, recordForAddrPart1, mapHistory, err);
    if (keyOfBuilding)
    {
      static const string& zeroNumFloor = STRING_ZERO;
      static const string& zeroNumHo = STRING_ZERO;
      buildDMAPKey(keyOfBuilding, zeroNumFloor, zeroNumHo, dmapKey);
#ifdef _DEBUG_MODE_
      return true;
#endif // _DEBUG_MODE_
    }
    keyOfBuilding = searchInAddrPart2(currentRecordMapPtr, recordForAddrPart1, recordForAddrPart2, mapHistory, err);
    if (keyOfBuilding)
    {
      string numFloor;
      string numHo;
      getNumFloorAndNumHo(keyOfBuilding, recordForAddrPart2, numFloor, numHo);

      buildDMAPKey(keyOfBuilding, numFloor, numHo, dmapKey);
#ifdef _DEBUG_MODE_
//      err << "result key: " << result << endl;
//      cout << err.str() << endl;
#endif // _DEBUG_MODE_
      return true;
    }

#ifdef _DEBUG_MODE_
    cout << "Failed" << endl << err.str() << endl;
#endif // _DEBUG_MODE_

    return false;
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toString() const
  {
    return primaryMapPtr_->toString();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toSimpleString() const
  {
    return primaryMapPtr_->toSimpleString();
  }

  _DEBUG_NO_INLINE_ATTRIBUTE_
  string toStringForInternalNodes() const
  {
    return primaryMapPtr_->toStringForInternalNodes();
  }

  void buildDummyDMAPKey(string& dmapKey) const
  {
    dmapKey.clear();
    ostringstream os;
    /* 주소 구분 코드 (1) */
    os << 1;
    /* 시도군 코드 (5) + 법정동 코드 (5)*/
    /* 도로명 번호 (7) */
    /* 지하 여부 (1) */
    /* 건물 본번 (5) */
    /* 건물 부번 (5) */
    /* 동 (8) */
    /* 층 (8) */
    /* 호 (4) */
    const size_t len = 5 + 5 + 7 + 1 + 5 + 5 + 8 + 8 + 4;
    appendDMAPKeyAttribute(os, len, STRING_ZERO);
    dmapKey = os.str();
  }

private:
  void getNumFloorAndNumHo(const char* keyOfBuilding, const Record& recordForAddrPart2, string& numFloor, string& numHo) const
  {
    numFloor = numHo = STRING_ZERO;
    BuildingKeyMap::const_iterator itr = buildingKeyMap_.find(keyOfBuilding);
    if (buildingKeyMap_.end() != itr)
    {
      bool foundHo = false;
      bool foundFloor = false;
      Record::const_reverse_iterator rItr = recordForAddrPart2.rbegin();
      for (; rItr != recordForAddrPart2.rend(); ++rItr)
      {
        const string& candidate = **rItr;
        if (utility::regex::getHoNumber(numHo, candidate.c_str()))
        {
          foundHo = true;
          break;
        }
      }

      if (! foundHo)
      {
        rItr = recordForAddrPart2.rbegin();
        for (; rItr != recordForAddrPart2.rend(); ++rItr)
        {
          const string& candidate = **rItr;
          if (utility::regex::getFloorNumber(numFloor, candidate.c_str()))
          {
            // cout << "floor:" << numFloor << endl;
            foundFloor = true;
            break;
          }
        }
      }

      if (! foundHo)
      {
        RecordPtr recordPtr(itr->second);
        const StringPtr nameDetailedBD = recordPtr->at(CAST(IndexesOfRecord::NAME_DETAILED_BD));
        assert(nameDetailedBD->size() <= utility::convertToUnicodeSize(100));
        string numDong;
        if (! utility::regex::getNumberFromDetailedBuilding(numDong, nameDetailedBD->c_str()))
        {
          rItr = recordForAddrPart2.rbegin();
          for (; rItr != recordForAddrPart2.rend(); ++rItr)
          {
            const string& candidate = **rItr;
            if (utility::regex::isNumber(candidate.c_str()))
            {
              numHo = candidate;
              foundHo = true;
              break;
            }
          }
        }
      }

      if (foundHo && ! foundFloor)
      {
        if (numHo.size() >= 8)
        {
          numHo = STRING_ZERO;
        }
        assert(utility::regex::isNumber(numHo) && numHo.size() < 8);
        const long long ho = atoll(numHo.c_str());
        ostringstream os;
        os << ho / 100;
        numFloor = os.str();
      }
      // cout << "floor:" << numFloor << ", ho: " << numHo << endl;
    }
    else
    {
      UNEXPECTE_ERROR;
    }
  }

  void appendDMAPKeyAttribute(ostream& os, const size_t expectedSize, const string& attribute) const
  {
    if (attribute.size() > expectedSize)
    {
      os << utility::StreamZeroPadding(expectedSize) << STRING_ZERO;
    }
    else
    {
      assert(attribute.size() <= expectedSize);
      assert(attribute.empty() || utility::regex::isNumber(attribute));
      os << utility::StreamZeroPadding(expectedSize) << attribute;
    }
  }

  void buildDMAPKey(const char* keyOfBuilding, const string& numFloor, const string& numHo, string& dmapKey) const
  {
    dmapKey.clear();
    BuildingKeyMap::const_iterator itr = buildingKeyMap_.find(keyOfBuilding);
    if (buildingKeyMap_.end() != itr)
    {
      RecordPtr recordPtr(itr->second);
      ostringstream os;
      /* 주소 구분 코드 (1) */
      os << 1;
      /* 시도군 코드 (5) + 법정동 코드 (5)*/
      const string& codeBDong = *recordPtr->at(CAST(IndexesOfRecord::CODE_B_DONG));
      appendDMAPKeyAttribute(os, 10, codeBDong);
      /* 도로명 번호 (7) */
      const string& codeRoad = *recordPtr->at(CAST(IndexesOfRecord::CODE_ROAD));
      assert(codeRoad.size() == 12);
      assert(utility::regex::isNumber(codeRoad));
      appendDMAPKeyAttribute(os, 7, codeRoad.substr(5, 7));
      /* 지하 여부 (1) */
      appendDMAPKeyAttribute(os, 1, *recordPtr->at(CAST(IndexesOfRecord::FLAG_BASEMENT)));
      /* 건물 본번 (5) */
      appendDMAPKeyAttribute(os, 5, *recordPtr->at(CAST(IndexesOfRecord::CODE_BUILDING1)));
      /* 건물 부번 (5) */
      appendDMAPKeyAttribute(os, 5, *recordPtr->at(CAST(IndexesOfRecord::CODE_BUILDING2)));
      /* 동 (8) */
      const StringPtr nameDetailedBD = recordPtr->at(CAST(IndexesOfRecord::NAME_DETAILED_BD));
      assert(nameDetailedBD->size() <= utility::convertToUnicodeSize(100));
      string numDong;
      if (! utility::regex::getNumberFromDetailedBuilding(numDong, nameDetailedBD->c_str()))
      {
        numDong.clear();
      }
      appendDMAPKeyAttribute(os, 8, numDong);
      /* 층 (8) */
      appendDMAPKeyAttribute(os, 8, numFloor);
      /* 호 (4) */
      appendDMAPKeyAttribute(os, 4, numHo);
      dmapKey = os.str();
    }
    else
    {
      UNEXPECTE_ERROR;
    }
  }

  void buildTryTokens(StringVector& tryTokens
                    , StringVector& nextTryTokens
                    , const char* inputToken) const
  {
    string s;
    StringVector stringVector;
    if (utility::regex::getNumber(s, inputToken)) tryTokens.push_back(s);
    if (utility::regex::getAlternativesForAptDong(stringVector, inputToken)) tryTokens.insert(tryTokens.end(), stringVector.begin(), stringVector.end());
    if (utility::regex::getAlternativesForHangJungDong(stringVector, inputToken)) tryTokens.insert(tryTokens.end(), stringVector.begin(), stringVector.end());
    if (utility::regex::getNumbersForBeonjiAndHo(stringVector, inputToken))
    {
      tryTokens.push_back(stringVector[0]);
      if (stringVector.size() == 2)
      {
        nextTryTokens.push_back(stringVector[1]);
      }
    }
    if (utility::regex::isNumber(inputToken))
    {
      s = inputToken;
      s.append("동");
      tryTokens.push_back(s);
    }
  }

  typedef SizeVector OrderList;
  void insert(const RecordPtr recordPtr
            , const OrderList& orderList
            , const size_t indexOfValue)
  {
    ConcurrentCharStringChainMapPtr currentRecordMapPtr(primaryMapPtr_);
    ConcurrentCharStringChainMapPtrVector mapHistory;

    for (auto itr = orderList.begin(); itr != orderList.end(); ++itr)
    {
      assert(currentRecordMapPtr);
      Element element;
      if (orderList.end() == (itr+1))
      {
        element.value_ = recordPtr->at(indexOfValue)->c_str();
      }

      const size_t index = *itr;
      const string& key = *recordPtr->at(index);
      registerElement(currentRecordMapPtr, key.c_str(), element, mapHistory);
    }
  }

  void insertNewAddr1(const RecordPtr recordPtr, const size_t indexOfValue)
  {
    static const OrderList orderList = {
      CAST(IndexesOfRecord::NAME_SI_DO)      /* 시도명 */,
      CAST(IndexesOfRecord::NAME_SI)         /* 시명 */,
      CAST(IndexesOfRecord::NAME_GUN_GU)     /* 군구명 */,
      CAST(IndexesOfRecord::NAME_ROAD)       /* 도로명 */,
      CAST(IndexesOfRecord::CODE_BUILDING1)  /* 건물본번 */,
      CAST(IndexesOfRecord::CODE_BUILDING2)  /* 건물부번 */,
      CAST(IndexesOfRecord::NAME_DETAILED_BD)/* 상세건물명 */,
    };
    insert(recordPtr, orderList, indexOfValue);
  }

  void insertNewAddr2(const RecordPtr recordPtr, const size_t indexOfValue)
  {
    static const OrderList orderList = {
      CAST(IndexesOfRecord::NAME_SI_DO)      /* 시도명 */,
      CAST(IndexesOfRecord::NAME_SI)         /* 시명 */,
      CAST(IndexesOfRecord::NAME_GUN_GU)     /* 군구명 */,
      CAST(IndexesOfRecord::NAME_B_E_M_DONG) /* 법정읍면동명 */,
      CAST(IndexesOfRecord::NAME_ROAD)       /* 도로명 */,
      CAST(IndexesOfRecord::CODE_BUILDING1)  /* 건물본번 */,
      CAST(IndexesOfRecord::CODE_BUILDING2)  /* 건물부번 */,
      CAST(IndexesOfRecord::NAME_DETAILED_BD)/* 상세건물명 */,
    };
    insert(recordPtr, orderList, indexOfValue);
  }

  void insertOldAddr1(const RecordPtr recordPtr, const size_t indexOfValue)
  {
    static const OrderList orderList = {
      CAST(IndexesOfRecord::NAME_SI_DO)      /* 시도명 */,
      CAST(IndexesOfRecord::NAME_SI)         /* 시명 */,
      CAST(IndexesOfRecord::NAME_GUN_GU)     /* 군구명 */,
      CAST(IndexesOfRecord::NAME_B_E_M_DONG) /* 법정읍면동명 */,
      CAST(IndexesOfRecord::NAME_B_L)        /* 법정리명 */,
      CAST(IndexesOfRecord::CODE_BUNJI1)     /* 지번본번(번지) */,
      CAST(IndexesOfRecord::CODE_BUNJI2)     /* 지번부호(호) */,
      CAST(IndexesOfRecord::NAME_DETAILED_BD)/* 상세건물명 */,
    };
    insert(recordPtr, orderList, indexOfValue);
  }

  void insertOldAddr2(const RecordPtr recordPtr, const size_t indexOfValue)
  {
    static const OrderList orderList = {
      CAST(IndexesOfRecord::NAME_SI_DO)      /* 시도명 */,
      CAST(IndexesOfRecord::NAME_SI)         /* 시명 */,
      CAST(IndexesOfRecord::NAME_GUN_GU)     /* 군구명 */,
      CAST(IndexesOfRecord::NAME_DONG)       /* 행정동명 */,
      CAST(IndexesOfRecord::NAME_B_L)        /* 법정리명 */,
      CAST(IndexesOfRecord::CODE_BUNJI1)     /* 지번본번(번지) */,
      CAST(IndexesOfRecord::CODE_BUNJI2)     /* 지번부호(호) */,
      CAST(IndexesOfRecord::NAME_DETAILED_BD)/* 상세건물명 */,
    };
    insert(recordPtr, orderList, indexOfValue);
  }

  void registerElement(ConcurrentCharStringChainMapPtr& currentRecordMapPtr
                     , const char* key
                     , Element& element
                     , ConcurrentCharStringChainMapPtrVector& mapHistory)
  {
    if (element.isEmpty())
    {
      registerElement(currentRecordMapPtr, key, element);
    }
    else
    {
      registerElement_(currentRecordMapPtr, key, element);
    }

    assert(! element.isEmpty());

    if (element.nextMap_)
    {
      mapHistory.push_back(currentRecordMapPtr);
      currentRecordMapPtr = element.nextMap_;
    }
  }

  void registerElement(ConcurrentCharStringChainMapPtr& map
                     , const char* key
                     , Element& element)
  {
//    unique_lock<mutex> mlock(mutex_);
//    ConcurrentCharStringChainMap::const_iterator result = map->find(key);
    const Element* elementPtr = map->getValuePtrOnLock(key);
    if (nullptr != elementPtr)
    {
      element = *elementPtr;
    }
    else
    {
      if (! element.value_)
      {
        ConcurrentCharStringChainMapPtr concurrentRecordMapPtr(new ConcurrentCharStringChainMap());
        concurrentRecordMapPtrConcurrentQueue_.push(concurrentRecordMapPtr);
        element.nextMap_ = concurrentRecordMapPtr;

        if (map.get() == primaryMapPtr_.get())
        {
          AliasMap::const_iterator itr = aliasMap_.find(key);
          if (aliasMap_.end() != itr)
          {
            for (string s : itr->second)
            {
              registerElement_(map, s.c_str(), element);
            }
          }
        }
      }
      registerElement_(map, key, element);
    }
  }

  void registerElement_(ConcurrentCharStringChainMapPtr& map, const char* key, Element& element)
  {
    assert(! element.isEmpty());
    assert(map != element.nextMap_);
    map->insert(make_pair(key, element));
  }

  void bypassTransientNode(ConcurrentCharStringChainMapPtr& currentRecordMapPtr
                         , stringstream& err) const
  {
    ConcurrentCharStringChainMap::const_iterator itr = currentRecordMapPtr->find("");
    if (currentRecordMapPtr->end() != itr)
    {
      currentRecordMapPtr = itr->second.nextMap_;
    }
  }

  bool tryToSearchForFirstToken(ConcurrentCharStringChainMapPtr& currentRecordMapPtr
                              , const char* currentToken
                              , stringstream& err) const
  {
    ConcurrentCharStringChainMap::const_iterator nextItr = secondaryMapPtr_->find(currentToken);
    if (secondaryMapPtr_->end() != nextItr)
    {
#ifdef _DEBUG_MODE_
      printTokenStatus(secondaryMapPtr_, currentToken, err, "tryToSearchForFirstToken");
#endif // _DEBUG_MODE_
      currentRecordMapPtr = nextItr->second.nextMap_;
      return true;
    }

    return false;
  }

#ifdef _DEBUG_MODE_
  void printCurrentTokenStatus(const ConcurrentCharStringChainMapPtr currentRecordMapPtr
                             , const char* currentToken
                             , stringstream& err) const
  {
    printTokenStatus(currentRecordMapPtr, currentToken, err, "currentToken");
  }

  void printTokenStatus(const ConcurrentCharStringChainMapPtr currentRecordMapPtr
                      , const char* currentToken
                      , stringstream& err
                      , const char* tokenType) const
  {
    const bool found = currentRecordMapPtr->end() != currentRecordMapPtr->find(currentToken);
    err << (found ? "Found. " : "Not found. ")
        << tokenType << ": {" << currentToken << "} , candidates: "
        << currentRecordMapPtr->toStringForKey() << endl;
  }
#endif // _DEBUG_MODE_

  const char* searchInAddrPart1(ConcurrentCharStringChainMapPtr& currentRecordMapPtr
                              , const Record& recordForAddrPart1
                              , ConcurrentCharStringChainMapPtrVector& mapHistory
                              , stringstream& err) const
  {
    Record::const_iterator rItr = recordForAddrPart1.begin();

    for (; rItr != recordForAddrPart1.end(); ++rItr)
    {
      mapHistory.push_back(currentRecordMapPtr);
      StringVector tryTokens;
      const char* currentToken = (*rItr)->c_str();
      if (strlen(currentToken) == 0)
      {
        continue;
      }

      ConcurrentCharStringChainMap::const_iterator mItr = currentRecordMapPtr->find(currentToken);
      //cout << "[token:" << currentToken << "] " << currentRecordMapPtr->toStringForKey() << endl;
#ifdef _DEBUG_MODE_
      printCurrentTokenStatus(currentRecordMapPtr, currentToken, err);
#endif // _DEBUG_MODE_

      if (currentRecordMapPtr->end() == mItr)
      {
        // Bypass the first token(e.g. 서울, 경기, ...) when matching fails
        if (recordForAddrPart1.begin() == rItr)
        {
          if (tryToSearchForFirstToken(currentRecordMapPtr, currentToken, err))
          {
            continue;
          }
        }

        // skip an intermediate node
        // move to next map if the expected key is empty
        static const StringVector tryTokens = {""};
        bool bFound = false;
          for (auto& s : tryTokens)
          {
            mItr = currentRecordMapPtr->find(s.c_str());
#ifdef _DEBUG_MODE_
            {
              IndentingOStreambuf ios(err);
              printTokenStatus(currentRecordMapPtr, currentToken, err, "bypassToken");
            }
#endif // _DEBUG_MODE_
            if (currentRecordMapPtr->end() != mItr)
            {
              if (mItr->second.nextMap_)
              {
                bFound = true;
                currentRecordMapPtr = mItr->second.nextMap_;
              }
              break;
            }
          }

        //cout << "[token:" << currentToken << "] " << currentRecordMapPtr->toStringForKey() << endl;
        if (bFound)
        {
#ifdef _DEBUG_MODE_
          printCurrentTokenStatus(currentRecordMapPtr, currentToken, err);
#endif // _DEBUG_MODE_
          mItr = currentRecordMapPtr->findOrGetNext(currentToken);
        }
      }

      if (currentRecordMapPtr->end() != mItr)
      {
        if (mItr->second.nextMap_)
        {
          currentRecordMapPtr = mItr->second.nextMap_;
        }
        else
        {
          return mItr->second.value_;
        }
      }
    }

    return nullptr;
  }

  const char* searchInAddrPart2(ConcurrentCharStringChainMapPtr& currentRecordMapPtr
                              , const Record& recordForAddrPart1
                              , const Record& recordForAddrPart2
                              , ConcurrentCharStringChainMapPtrVector& mapHistory
                              , stringstream& err) const
  {
    Record::const_iterator rItr = recordForAddrPart2.begin();
    ConcurrentCharStringChainMap::const_iterator mItr;
    const char* currentToken = (*rItr)->c_str();
    StringVector tryTokens;
    StringVector nextTryTokens;

    for (; rItr != recordForAddrPart2.end(); ++rItr)
    {
      mapHistory.push_back(currentRecordMapPtr);
      currentToken = (*rItr)->c_str();
      if (strlen(currentToken) == 0)
      {
        continue;
      }
#ifdef _DEBUG_MODE_
      printCurrentTokenStatus(currentRecordMapPtr, currentToken, err);
#endif // _DEBUG_MODE_
      mItr = currentRecordMapPtr->find(currentToken);

      if (currentRecordMapPtr->end() == mItr)
      {
        // skip an intermediate node
        // move to next map if the expected key is either {empty|0}
        static const StringVector tryTokens = {"", "0"};
        bool bFound = false;
        for (auto& s : tryTokens)
        {
          mItr = currentRecordMapPtr->find(s.c_str());
#ifdef _DEBUG_MODE_
          {
            IndentingOStreambuf ios(err);
            printTokenStatus(currentRecordMapPtr, currentToken, err, "bypassToken");
          }
#endif // _DEBUG_MODE_
          if (currentRecordMapPtr->end() != mItr)
          {
            if (mItr->second.nextMap_)
            {
              bFound = true;
              currentRecordMapPtr = mItr->second.nextMap_;
            }
            break;
          }
        }

        if (bFound)
        {
#ifdef _DEBUG_MODE_
          printCurrentTokenStatus(currentRecordMapPtr, currentToken, err);
#endif // _DEBUG_MODE_
          mItr = currentRecordMapPtr->findOrGetNext(currentToken);
        }
      }

      if (currentRecordMapPtr->end() == mItr)
      {
        // handling exceptional cases
        // try again with possible cases if not found any element
        tryTokens.clear();
        nextTryTokens.clear();
        buildTryTokens(tryTokens, nextTryTokens, currentToken);
        for (auto& token : tryTokens)
        {
#ifdef _DEBUG_MODE_
          err << "tryToken: [" << token << "]" << endl;
#endif // _DEBUG_MODE_
          mItr = currentRecordMapPtr->findOrGetNext(token.c_str());
//          err << currentRecordMapPtr->toString() << endl;
          if (currentRecordMapPtr->end() != mItr)
          {
#ifdef _DEBUG_MODE_
            err << "Found match from tryTokens: " << token << endl;
#endif // _DEBUG_MODE_
            if (! nextTryTokens.empty())
            {
              if (mItr->second.nextMap_)
              {
                currentRecordMapPtr = mItr->second.nextMap_;
                for (auto& nextToken : nextTryTokens)
                {
#ifdef _DEBUG_MODE_
                  err << "nextTryToken: [" << nextToken << "]" << endl;
#endif // _DEBUG_MODE_
                  mItr = currentRecordMapPtr->findOrGetNext(nextToken.c_str());
                  if (currentRecordMapPtr->end() != mItr)
                  {
#ifdef _DEBUG_MODE_
                    err << "Found match from tryTokens: " << token << endl;
#endif // _DEBUG_MODE_
                    break;
                  }
                }
              }
            }

            break;
          }
        }
      }

      if (currentRecordMapPtr->end() != mItr)
      {
        if (mItr->second.value_)
        {
          return mItr->second.value_;
        }
        else
        {
          currentRecordMapPtr = mItr->second.nextMap_;
          assert(currentRecordMapPtr);
        }
      }
      else
      {
#ifdef _DEBUG_MODE_
        printCurrentTokenStatus(currentRecordMapPtr, currentToken, err);
#endif // _DEBUG_MODE_
      }
    }

    mItr = currentRecordMapPtr->findOrGetNext("");
    if (currentRecordMapPtr->end() != mItr)
    {
      if (mItr->second.value_)
      {
        return mItr->second.value_;
      }
    }

#ifdef _DEBUG_MODE_
    printCurrentTokenStatus(currentRecordMapPtr, "", err);
#endif // _DEBUG_MODE_

    return nullptr;
  }

  void buildAliasMap()
  {
   const StringVectorVector aliasesList = {
      {"강원도", "강원"},
      {"경기도", "경기"},
      {"경상남도", "경남"},
      {"경상북도", "경북"},
      {"광주광역시", "광주", "광주시"},
      {"대구광역시", "대구", "대구시"},
      {"대전광역시", "대전", "대전시"},
      {"부산광역시", "부산", "부산시"},
      {"서울특별시", "서울", "서울시"},
      {"세종특별자치시", "세종", "세종시"},
      {"울산광역시", "울산", "울산시"},
      {"인천광역시", "인천", "인천시"},
      {"전라남도", "전남"},
      {"전라북도", "전북"},
      {"제주특별자치도", "제주"},
      {"충청남도", "충남"},
      {"충청북도", "충북"},
    };

    for (auto aliasOfSido : aliasesList)
    {
      const string& key = *aliasOfSido.begin();
      aliasMap_.insert(make_pair(key, StringVector(aliasOfSido.begin()+1, aliasOfSido.end())));
    }
  }

private:
  ConcurrentCharStringChainMapPtrConcurrentQueue concurrentRecordMapPtrConcurrentQueue_;
  ConcurrentCharStringChainMapPtr primaryMapPtr_;
  ConcurrentCharStringChainMapPtr secondaryMapPtr_;
  BuildingKeyMap buildingKeyMap_;
  AliasMap aliasMap_;
  mutex mutex_;
};

class DictionaryBuilder
{
public:
  class SharedQueues
  {
  public:
    SharedQueues(const size_t numBlocks = 100
               , const size_t numElementsInBlock = 10)
    {
      for (size_t i = 0; i < numBlocks; ++i)
      {
        StringPtrVectorPtr stringPtrVectorPtr(new StringPtrVector());
        for (size_t j = 0; j < numElementsInBlock; ++j)
        {
          StringPtr stringPtr(new string());
          stringPtrVectorPtr->push_back(stringPtr);
        }

        emptyJobs.push(stringPtrVectorPtr);
      }
    }

    ~SharedQueues()
    {
      cout << "Destroyed sharedQueue" << endl;
    }

    _DEBUG_NO_INLINE_ATTRIBUTE_
    string toString()
    {
      stringstream ss;
      ss << "{todoJobs: " << todoJobs.toString()
         << "}, {emptyJobs: " << emptyJobs.toString()
         << "}";
      return ss.str();
    }

    StringPtrVectorPtrConcurrentQueue todoJobs;
    StringPtrVectorPtrConcurrentQueue emptyJobs;
  };
  typedef shared_ptr<SharedQueues> SharedQueuesPtr;

  class Context
  {
  public:
    Context(const GlobalContext& globalContext)
      : terminate_(false)
      , globalContext_(globalContext)
      , sharedQueuesPtr_(new SharedQueues())
      , dictionaryPtr_(new Dictionary())
      , recordPtrVectorPtrConcurrentQueuePtr_(new RecordPtrVectorPtrConcurrentQueue())
    {
    }

    bool terminate_;
    const GlobalContext& globalContext_;
    SharedQueuesPtr sharedQueuesPtr_;
    DictionaryPtr dictionaryPtr_;
    RecordPtrVectorPtrConcurrentQueuePtr recordPtrVectorPtrConcurrentQueuePtr_;
  };

  RecordPtrVectorPtrConcurrentQueuePtr getStoredRecords() const
  {
    return context_.recordPtrVectorPtrConcurrentQueuePtr_;
  }

  class Processor : public AbstractThread
  {
  public:
    Processor(size_t id
            , Context& context)
      : AbstractThread(id)
      , context_(context)
    {
    }

    virtual ~Processor()
    {
    }

    virtual void run() const override
    {
      static const SizeVector requiredIndexes = {
        CAST(IndexesOfBuilding::CODE_B_DONG)            /* 1  법정동 코드 */,
        CAST(IndexesOfBuilding::NAME_SI_DO)             /* 2  시도명 */,
        CAST(IndexesOfBuilding::NAME_SI_GUN_GU)         /* 3  시군구명 => {1:시, 2:군/구} */,
        CAST(IndexesOfBuilding::NAME_B_E_M_DONG)        /* 4  법정읍면동명 */,
        CAST(IndexesOfBuilding::NAME_B_L)               /* 5  법정리명 */,
        CAST(IndexesOfBuilding::FLAG_SAN)               /* 6  산여부 => {0:대지, 1:산} */,
        CAST(IndexesOfBuilding::CODE_BUNJI1)            /* 7  지번본번(번지) */,
        CAST(IndexesOfBuilding::CODE_BUNJI2)            /* 8  지번부번(호) */,
        CAST(IndexesOfBuilding::CODE_ROAD)              /* 9  도로명코드 = 시군구코드(5) + 도로명번호(7) */,
        CAST(IndexesOfBuilding::NAME_ROAD)              /* 10 도로명 */,
        CAST(IndexesOfBuilding::FLAG_BASEMENT)          /* 11 지하여부 => {0:지상, 1:지하, 2:공중} */,
        CAST(IndexesOfBuilding::CODE_BUILDING1)         /* 12 건물본번 */,
        CAST(IndexesOfBuilding::CODE_BUILDING2)         /* 13 건물부번 */,
        CAST(IndexesOfBuilding::NAME_BUILDING)          /* 14 건축물대장 건물명 */,
        CAST(IndexesOfBuilding::NAME_DETAILED_BD)       /* 15 상세건물명 */,
        CAST(IndexesOfBuilding::CODE_BUILDING_PK)       /* 16 건물관리번호 (PK) */,
        CAST(IndexesOfBuilding::CODE_SEQ_E_M_DONG)      /* 17 읍면동일련번호 */,
        CAST(IndexesOfBuilding::CODE_DONG)              /* 18 행정동코드 */,
        CAST(IndexesOfBuilding::NAME_DONG)              /* 19 행정동명 */,
      };

      // cout << "Processor : tid = {" << id_ << "} has been started!" << endl;
      while (true)
      {
        if (context_.terminate_
            && context_.sharedQueuesPtr_->todoJobs.empty()) break;

        StringPtrVectorPtr stringPtrVectorPtr = context_.sharedQueuesPtr_->todoJobs.pop();
        if (stringPtrVectorPtr)
        {
          RecordPtrVectorPtr recordPtrVectorPtr(new RecordPtrVector());
          for (const auto& s : *stringPtrVectorPtr)
          {
            if (s->empty()) continue;
            RecordPtr recordPtr(new Record(metadataForNewAddr));
            recordPtrVectorPtr->push_back(recordPtr);
            LineSplitter::parse(s->c_str(), *recordPtr, requiredIndexes);
            if (recordPtr->at(CAST(IndexesOfBuilding::NAME_SI_GUN_GU))->find(CHAR_SINGLE_SPACE) != string::npos)
            {
              // split "시군구명" into "시" and "군구명"
              StringVector stringVector;
              LineSplitter::parse(recordPtr->at(CAST(IndexesOfBuilding::NAME_SI_GUN_GU))->c_str()
                  , stringVector, CHAR_SINGLE_SPACE);
              assert(stringVector.size() == 2);
              StringPtr si(new string(stringVector.at(0)));
              recordPtr->at(CAST(IndexesOfRecord::NAME_SI)) = si;
              StringPtr gungu(new string(stringVector.at(1)));
              recordPtr->insert(recordPtr->begin()+3, gungu);
            }
            else
            {
              recordPtr->insert(recordPtr->begin()+3, globalEmptyString);
            }
            //cout << recordPtr->toString() << endl;

            s->clear();
            context_.dictionaryPtr_->insert(recordPtr);
          }
          context_.recordPtrVectorPtrConcurrentQueuePtr_->push(recordPtrVectorPtr);
          context_.sharedQueuesPtr_->emptyJobs.push(stringPtrVectorPtr);
        }
        else
        {
          this_thread::sleep_for(std::chrono::milliseconds(MILLISECONDS_FOR_SLEEP));
        }
      }

      // cout << "Processor : tid = {" << id_ << "} has been terminateped!" << endl;
    }

  private:
    Context& context_;
  };
  typedef shared_ptr<Processor> ProcessorPtr;

  class Reader : public AbstractThread
  {
  public:
    Reader(size_t id
         , Context& context)
      : AbstractThread(id)
      , context_(context)
    {
    }

    virtual ~Reader()
    {
    }

    virtual void run() const override
    {
      // cout << "Reader: tid = {" << id_ << "} has been started!" << endl;
      const GlobalContext& globalContext = context_.globalContext_;
      PathVector sourceAddrFilePathVector;
      utility::getFilePaths(globalContext.getSourceDirectory(), string("build"), sourceAddrFilePathVector);
      //utility::getFilePaths(globalContext.getSourceDirectory(), string("test"), sourceAddrFilePathVector);
      for (auto& path : sourceAddrFilePathVector)
      {
//        if (path.string().find("gwangju") == string::npos) continue;
//        if (path.string().find("gyunggi") == string::npos) continue;
//        if (path.string().find("seoul") == string::npos) continue;
        cout << "File: " << path.filename() << endl;
        ifstream infile(path.string());
        bool isEOF;
        while (true)
        {
          StringPtrVectorPtr stringPtrVectorPtr = context_.sharedQueuesPtr_->emptyJobs.pop();
          if (stringPtrVectorPtr)
          {
            for (auto& s : *stringPtrVectorPtr)
            {
              isEOF = ! getline(infile, *s);
              if (isEOF)
              {
                break;
              }
            }
            context_.sharedQueuesPtr_->todoJobs.push(stringPtrVectorPtr);
          }
          else
          {
            this_thread::sleep_for(std::chrono::milliseconds(MILLISECONDS_FOR_SLEEP));
          }

          if (isEOF)
          {
            break;
          }
        }
      }

      context_.terminate_ = true;
    }

  private:
    Context& context_;
  };
  typedef shared_ptr<Reader> ReaderPtr;

  DictionaryPtr build()
  {
    const utility::ScopeDuration prepareDuration("* Elapsed time for build ");
    size_t tid = 0;
    const size_t numCores = utility::getNumCores();
    assert(numCores >= MINIMUM_NUM_CORES);
    ThreadPtrVector threadPtrVector;

    ReaderPtr readerPtr(new Reader(tid++, context_));
    threadPtrVector.push_back(ThreadPtr(new thread(&Reader::run, readerPtr)));

    for (; tid < numCores; ++tid)
    {
      ProcessorPtr processorPtr(new Processor(tid, context_));
      threadPtrVector.push_back(ThreadPtr(new thread(&Processor::run, processorPtr)));
    }

    for (auto& threadPtr : threadPtrVector)
    {
      if (threadPtr && threadPtr->joinable())
      {
        threadPtr->join();
      }
    }

    context_.dictionaryPtr_->buildSecondaryMap();

    return context_.dictionaryPtr_;
  }

  DictionaryBuilder(const GlobalContext& globalContext)
    : context_(globalContext)
  {
  }

  virtual ~DictionaryBuilder()
  {
  }

private:
  Context context_;
};
typedef shared_ptr<DictionaryBuilder> DictionaryBuilderPtr;

// TODO: support BNF
class InputAddressParser
{
public:

  static void parse(const char* line, RecordRef recordRef)
  {
    assert(line);

    recordRef.clear();
    const char* prevPos = line;
    const char* currPos = prevPos;
    const Metadata& metadata = recordRef.getMetadata();

    // TODO: support multiple separators in metadata
    CharVector separators;
    separators.push_back(metadata.getSeparator());
    separators.push_back(',');
    separators.push_back('-');

    const size_t bufSize = 1024;
    char buf[bufSize];
    for (; '\0' != *currPos; currPos++)
    {
      if (hasSeparator(*currPos, separators))
      {
        // Exceptional handling
        // : to avoid continuative separatos
        // e.g. 101                201
        if (*prevPos == ' ' && (*prevPos == *currPos))
        {
          prevPos = currPos+1;
          continue;
        }

        const size_t length = currPos - prevPos;
        assert(length < bufSize);
        utility::safeStrncpy(buf, prevPos, length);
        shared_ptr<string> s(new string(buf));
        recordRef.push_back(s);
        prevPos = currPos+1;
      }
    }

    if (hasSeparator(*(currPos-1), separators))
    {
      recordRef.push_back(globalEmptyString);
    }
    else
    {
      const size_t length = currPos - prevPos;
      assert(length < bufSize);
      utility::safeStrncpy(buf, prevPos, length);
      shared_ptr<string> s(new string(buf));
      recordRef.push_back(s);
    }
  }

private:
  static inline bool hasSeparator(const char c, const CharVector& separators)
  {
    for (auto& s : separators)
    {
      if (c == s) return true;
    }
    return false;
  }
};

class AddressRefiner
{
public:
  class SharedQueues
  {
  public:
    SharedQueues(const size_t numBlocks = 1000
               , const size_t numElementsInBlock = 1000)
      : numTotalJobs(numBlocks)
    {
      for (size_t i = 0; i < numBlocks; ++i)
      {
        StringPtrVectorPtr stringPtrVectorPtr(new StringPtrVector());
        for (size_t j = 0; j < numElementsInBlock; ++j)
        {
          StringPtr stringPtr(new string());
          stringPtrVectorPtr->push_back(stringPtr);
        }

        emptyJobs.push(stringPtrVectorPtr);
      }
    }

    _DEBUG_NO_INLINE_ATTRIBUTE_
    string toString()
    {
      stringstream ss;
      ss << "{todoJobs: " << todoJobs.toString()
         << "}, {emptyJobs: " << emptyJobs.toString()
         << "}, {doneJobs: " << doneJobs.toString()
         << "}";
      return ss.str();
    }

    _DEBUG_NO_INLINE_ATTRIBUTE_
    bool isDone()
    {
      return numTotalJobs == emptyJobs.size() && doneJobs.empty() && todoJobs.empty();
    }

    const size_t numTotalJobs;
    StringPtrVectorPtrConcurrentQueue doneJobs;
    StringPtrVectorPtrConcurrentQueue todoJobs;
    StringPtrVectorPtrConcurrentQueue emptyJobs;
  };
  typedef shared_ptr<SharedQueues> SharedQueuesPtr;

  class Context
  {
  public:
    Context(const GlobalContext& globalContext
          , const Dictionary& dictionary)
      : terminate_(false)
      , globalContext_(globalContext)
      , sharedQueuesPtr_(new SharedQueues())
      , dictionary_(dictionary)
      , numTotal_(0)
      , numFailures_(0)
    {
    }

    bool terminate_;
    const GlobalContext& globalContext_;
    SharedQueuesPtr sharedQueuesPtr_;
    const Dictionary& dictionary_;
    unsigned long numTotal_;
    unsigned long numFailures_;
   };

  class Processor : public AbstractThread
  {
  public:
    Processor(size_t id
            , Context& context)
      : AbstractThread(id)
      , context_(context)
      , numTotal_(new unsigned long)
      , numFailures_(new unsigned long)
    {
      *numTotal_ = 0;
      *numFailures_ = 0;
    }

    virtual ~Processor()
    {
    }

    virtual void run() const override
    {
      // cout << "Processor : tid = {" << id_ << "} has been started!" << endl;
      const Dictionary& dictionary = context_.dictionary_;
      Record tmpRecord(metadataForInput);

      unsigned long& numTotal = *numTotal_;
      unsigned long& numFailures = *numFailures_;
      while (true)
      {
        if (context_.terminate_
            && context_.sharedQueuesPtr_->isDone()) break;

        StringPtrVectorPtr stringPtrVectorPtr = context_.sharedQueuesPtr_->todoJobs.pop();
        if (stringPtrVectorPtr)
        {
          ostringstream osSuccesses;
          ostringstream osFailures;
          for (const auto& s : *stringPtrVectorPtr)
          {
            if (s->empty()) continue;
            LineSplitter::parse(s->c_str(), tmpRecord);
            s->clear();

            string dmapKey;
            bool found = false;
            const size_t numElementsInRecord = tmpRecord.size();
            if (numElementsInRecord <= INDEX_ADDR_PART2_FOR_INPUT)
            {
              found = true;
              dictionary.buildDummyDMAPKey(dmapKey);
            }
            else
            {
              Record recordForAddrPart1(metadataForAddrPart1);
              Record recordForAddrPart2(metadataForAddrPart2);
              assert(tmpRecord.size() >= INDEX_ADDR_PART1_FOR_INPUT);
              InputAddressParser::parse(
                  tmpRecord.at(INDEX_ADDR_PART1_FOR_INPUT)->c_str()
                  , recordForAddrPart1);
              assert(tmpRecord.size() >= INDEX_ADDR_PART2_FOR_INPUT);
              InputAddressParser::parse(
                  tmpRecord.at(INDEX_ADDR_PART2_FOR_INPUT)->c_str()
                  , recordForAddrPart2);

              //if (recordForAddrPart1.at(0)->compare("광주") != 0) continue;
              //if (recordForAddrPart1.at(0)->compare("경기") != 0) continue;
              //if (recordForAddrPart1.at(0)->compare("강남구") != 0) continue;

              found = dictionary.search(recordForAddrPart1, recordForAddrPart2, dmapKey);
            }

            ostringstream& os = (found ? osSuccesses : osFailures);

            _appendString(os, tmpRecord, INDEX_ADDR_KEY, numElementsInRecord);
            if (found)
            {
              os << dmapKey << SEPARATOR_FOR_INPUT;
            }
            else
            {
              _appendString(os, tmpRecord, INDEX_ADDR_PART1_FOR_INPUT, numElementsInRecord);
              _appendString(os, tmpRecord, INDEX_ADDR_PART2_FOR_INPUT, numElementsInRecord);
              numFailures++;
            }
            os << endl;
            numTotal++;
          }
          assert(stringPtrVectorPtr->size() > 2);
          (stringPtrVectorPtr->at(0))->append(osSuccesses.str());
          (stringPtrVectorPtr->at(1))->append(osFailures.str());
          context_.sharedQueuesPtr_->doneJobs.push(stringPtrVectorPtr);
        }
        else
        {
          this_thread::sleep_for(std::chrono::milliseconds(MILLISECONDS_FOR_SLEEP));
          //cout << "Processor: " << context_.sharedQueuesPtr_->toString() << endl;
        }
      }

      // cout << "Processor: " << context_.sharedQueuesPtr_->toString() << endl;
      // cout << "Processor : tid = {" << id_ << "} has been terminateped!" << endl;
    }

    void _appendString(ostream& os, const Record& record, const size_t currSize, const size_t maxSize) const
    {
      if (maxSize > currSize)
      {
        os << *record.at(currSize);
      }
      else
      {
        os << STRING_ZERO;
      }
      os << SEPARATOR_FOR_INPUT;
    }

    unsigned long getNumTotal() const
    {
      return *numTotal_;
    }

    unsigned long getNumFailures() const
    {
      return *numFailures_;
    }

  private:
    Context& context_;
    UnsignedLongPtr numTotal_;
    UnsignedLongPtr numFailures_;
  };
  typedef shared_ptr<Processor> ProcessorPtr;
  typedef vector<ProcessorPtr> ProcessorPtrVector;

  class Reader : public AbstractThread
  {
  public:
    Reader(size_t id
         , Context& context)
      : AbstractThread(id)
      , context_(context)
    {
    }

    virtual ~Reader()
    {
    }

    virtual void run() const override
    {
      // cout << "Reader: tid = {" << id_ << "} has been started!" << endl;
      const GlobalContext& globalContext = context_.globalContext_;
      PathVector inputFilePathVector;
      utility::getFilePaths(globalContext.getInputDirectory(), string("0"), inputFilePathVector);
      for (auto& path : inputFilePathVector)
      {
//        if (path.string().find("000100_0") == string::npos) continue;
        cout << "File: " << path.filename() << endl;
        ifstream infile(path.string());
        while (true)
        {
          bool isEOF = false;
          StringPtrVectorPtr stringPtrVectorPtr = context_.sharedQueuesPtr_->emptyJobs.pop();
          if (stringPtrVectorPtr)
          {
            for (auto& s : *stringPtrVectorPtr)
            {
              isEOF = ! getline(infile, *s);
              if (isEOF)
              {
                break;
              }
            }
            context_.sharedQueuesPtr_->todoJobs.push(stringPtrVectorPtr);
          }
          else
          {
            this_thread::sleep_for(std::chrono::milliseconds(MILLISECONDS_FOR_SLEEP));
            //cout << "Reader: " << context_.sharedQueuesPtr_->toString() << endl;
          }

          if (isEOF)
          {
            break;
          }
        }
//        break;
      }

      // cout << "Reader: " << context_.sharedQueuesPtr_->toString() << endl;

      context_.terminate_ = true;
    }

  private:
    Context& context_;
  };
  typedef shared_ptr<Reader> ReaderPtr;

  class Writer: public AbstractThread
  {
  public:
    Writer(size_t id
         , Context& context)
      : AbstractThread(id)
      , context_(context)
    {
    }

    virtual ~Writer()
    {
    }

    virtual void run() const override
    {
      // cout << "Writer: tid = {" << id_ << "} has been started!" << endl;
      const GlobalContext& globalContext = context_.globalContext_;
      const string& filepathForSuccesses = globalContext.getFilenameForSuccesses();
      const string& filepathForFailures = globalContext.getFilenameForFailures();
      ofstream fileHandlerForSuccesses(filepathForSuccesses);
      ofstream fileHandlerForFailures(filepathForFailures);

      while (true)
      {
        if (context_.terminate_
            && context_.sharedQueuesPtr_->isDone()) break;

        StringPtrVectorPtr stringPtrVectorPtr = context_.sharedQueuesPtr_->doneJobs.pop();
        if (stringPtrVectorPtr)
        {
          assert(stringPtrVectorPtr->size() > 2);
          string& strSuccesses = *stringPtrVectorPtr->at(0);
          string& strFailures = *stringPtrVectorPtr->at(1);
          if (! strSuccesses.empty() || ! strFailures.empty())
          {
            fileHandlerForSuccesses.write(strSuccesses.c_str(), strSuccesses.size());
            fileHandlerForFailures.write(strFailures.c_str(), strFailures.size());
          }
          context_.sharedQueuesPtr_->emptyJobs.push(stringPtrVectorPtr);
        }
        else
        {
          this_thread::sleep_for(std::chrono::milliseconds(MILLISECONDS_FOR_SLEEP));
          //cout << "Writer: " << context_.sharedQueuesPtr_->toString() << endl;
        }
      }

      fileHandlerForSuccesses.close();
      fileHandlerForFailures.close();
      // cout << "Writer: " << context_.sharedQueuesPtr_->toString() << endl;

      context_.terminate_ = true;
    }

  private:
    Context& context_;
  };
  typedef shared_ptr<Writer> WriterPtr;

  void convert()
  {
    const utility::ScopeDuration prepareDuration("* Elapsed time for conversion ");

    size_t tid = 0;
    const size_t numCores = utility::getNumCores() + 2;
    assert(numCores >= MINIMUM_NUM_CORES);
    ThreadPtrVector threadPtrVector;
    ProcessorPtrVector processorPtrVector;

    ReaderPtr readerPtr(new Reader(tid++, context_));
    threadPtrVector.push_back(ThreadPtr(new thread(&Reader::run, readerPtr)));

    WriterPtr writerPtr(new Writer(tid++, context_));
    threadPtrVector.push_back(ThreadPtr(new thread(&Writer::run, writerPtr)));

    for (; tid < numCores; ++tid)
    {
      ProcessorPtr processorPtr(new Processor(tid, context_));
      processorPtrVector.push_back(processorPtr);
      threadPtrVector.push_back(ThreadPtr(new thread(&Processor::run, processorPtr)));
    }

    for (auto& threadPtr : threadPtrVector)
    {
      if (threadPtr && threadPtr->joinable())
      {
        threadPtr->join();
      }
    }

    unsigned long numTotal = 0;
    unsigned long numFailures = 0;

    for (auto& processorPtr : processorPtrVector)
    {
      numTotal += processorPtr->getNumTotal();
      numFailures += processorPtr->getNumFailures();
      cout << "[" << processorPtr->getId() << "] total: " << processorPtr->getNumTotal() << ", failures: " << processorPtr->getNumFailures() << endl;
    }

    cout << "numTotal: " << numTotal << ", numFailures: " << numFailures << endl;
  }

  AddressRefiner(const GlobalContext& globalContext
                 , const Dictionary& dictionary)
    : context_(globalContext, dictionary)
  {
  }

private:
  Context context_;
};

bool verify(const string& sourceDirectory
          , const string& inputDirectory
          , const string& filepathForSuccesses
          , const string& filepathForFailures)
{
  stringstream ss;
  ss << "Program failed because of";
  if (filepathForSuccesses.compare(filepathForFailures) == 0)
  {
    ss << " the succeeded file path is equal to the failed file path" << endl;
    ss << "[succeeded filepath=" << filepathForSuccesses;
    ss << ", failed filepath=" << filepathForFailures;
    ss << "]";
    cerr << ss.str() << endl;
    return false;
  }
  if (! utility::isDirectory(sourceDirectory))
  {
    ss << " not found directory: " << sourceDirectory;
    cerr << ss.str() << endl;
    return false;
  }
  if (! utility::isDirectory(inputDirectory))
  {
    ss << " not found directory: " << inputDirectory;
    cerr << ss.str() << endl;
    return false;
  }
  if (! ofstream(filepathForSuccesses))
  {
    ss << " cannot create file: " << filepathForSuccesses;
    cerr << ss.str() << endl;
    return false;
  }
  if (! ofstream(filepathForFailures))
  {
    ss << " cannot create file: " << filepathForFailures;
    cerr << ss.str() << endl;
    return false;
  }

  return true;
}

int main(int argc, char*argv[])
{
//#define USE_DEFAULT_ARGUMENTS
#ifdef USE_DEFAULT_ARGUMENTS
  const string inputDirectory = "data/td_zngm_integ_txt_addr/dt=20170718";
  const string filepathForSuccesses = "data/result/successes.csv";
  const string filepathForFailures= "data/result/failures.csv";
#else
  // TODO: enhance arugments handling
  const int expectedLength = 4;
  if (argc < expectedLength)
  {
    stringstream ss;
    ss << "Program failed because of insufficient arguments:"
       << " <input directory path> <a path for succeeded results> < a path for failed results>"
       << endl;
    ss << "[expectedLength: " << expectedLength << " != currentLength: " << argc << "]";
    cerr << ss.str() << endl;
    return 1;
  }
  const string inputDirectory = argv[1];
  const string filepathForSuccesses = argv[2];
  const string filepathForFailures= argv[3];
#endif // USE_DEFAULT_ARGUMENTS

  const utility::ScopeDuration totalDuration("* Total elapsed time");
  const string sourceDirectory = "data/source";
  const size_t initialBucketSize = 20747804;
  const size_t keyIndex = 0;

  if (! verify(sourceDirectory, inputDirectory, filepathForSuccesses, filepathForFailures))
  {
    return 1;
  }

  GlobalContext globalContext(sourceDirectory
                            , inputDirectory
                            , filepathForSuccesses
                            , filepathForFailures
                            , initialBucketSize
                            , keyIndex);

  DictionaryBuilderPtr dictionaryBuilderPtr(new DictionaryBuilder(globalContext));
  DictionaryPtr dictionaryPtr = dictionaryBuilderPtr->build();
  RecordPtrVectorPtrConcurrentQueuePtr storedRecords
    = dictionaryBuilderPtr->getStoredRecords();

  //cout << dictionaryPtr->toSimpleString() << endl;
  //cout << dictionaryPtr->toStringForInternalNodes() << endl;
  //return 0;

  AddressRefiner addressConverter(globalContext, *dictionaryPtr);
  addressConverter.convert();

  return 0;
}
