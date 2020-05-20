#include <iostream>
#include <algorithm>
#include <memory>
#include <vector>
#include <stdint.h>
#include <fstream>

#if defined( __MINGW64__ ) // Чтобы собиралось на MinGW 7.3.0, который поставляется с Qt

#include <experimental/filesystem>
using namespace std::experimental;

#else

#include <filesystem>

#endif

using namespace std;

/**
 * @brief Класс для доступа к предварительно отсортированному фрагменту в файле
 */
class Chunk
{
        fstream& _f;
        size_t _readOffsetInBytes;
        vector<int64_t>::iterator _begin;
        size_t _maxElements;
        vector<int64_t>::iterator _nextInBuf;
        vector<int64_t>::iterator _realEnd;
        size_t _numbersToRead;

        void fillBuf()
        {
            if(_numbersToRead == 0)
                return;
            size_t numToRead = std::min(_maxElements, _numbersToRead);
            _f.seekg(_readOffsetInBytes);
            if(_f.fail())
            {
                _numbersToRead = 0;
                return;
            }
            _f.read(reinterpret_cast<char*>(&(*_begin)), numToRead * sizeof(int64_t));
            size_t numbersRead = _f.gcount() / sizeof(int64_t);
            _numbersToRead -= numbersRead;
            _realEnd = _begin + numbersRead;
            _readOffsetInBytes += numbersRead * sizeof(int64_t);
            if(_f.eof())
            {
                _numbersToRead = 0;
                _f.clear();
            }
            _nextInBuf = _begin;
        }

        bool isProcessed() const
        {
            return (_numbersToRead == 0) && (_nextInBuf == _realEnd);
        }

    public:
        /**
         * @brief Конструктор
         * @param f - ссылка на файловый поток, из которого читаем фрагмент
         * @param begin - итератор на первый элемент в массиве, в который можно сохранять вычитанные из файла данные
         * @param maxElements - максимальное количество чисел, которое можно прочитать в массив за раз
         * @param numbersInChunk - максимальное количество чисел int64_t в фрагменте
         * @param chunkIndex - номер фрагмента в файле по порядку
         */
        Chunk(fstream& f, vector<int64_t>::iterator begin, size_t maxElements, size_t numbersInChunk, size_t chunkIndex) :
            _f(f),
            _readOffsetInBytes(numbersInChunk * chunkIndex * sizeof(int64_t)),
            _begin(begin),
            _maxElements(maxElements),
            _nextInBuf(begin),
            _realEnd(begin),
            _numbersToRead(numbersInChunk)
        {}

        /**
         * @brief Получим следующее число из отсортированного фрагмента в файле
         * @param val - переменная, в которую будет сохранено значение полученного числа
         * @return true - число получили и сохранили в val, false - весь фрагмент уже вычитан
         */
        bool getNext(int64_t& val)
        {
            if(_nextInBuf == _realEnd)
                fillBuf();
            if(isProcessed())
                return false;
            val = *_nextInBuf;
            ++_nextInBuf;
            return true;
        }
};

/**
 * @brief Класс для буферизации записи в dst
 */
class ChunkWriter
{
        fstream& _dst;
        vector<int64_t>::iterator _begin;
        vector<int64_t>::iterator _end;
        vector<int64_t>::iterator _it;

        void flush()
        {
            _dst.write(reinterpret_cast<char*>(&(*_begin)), std::distance(_begin, _it) * sizeof(int64_t));
        }

    public:
        ChunkWriter(fstream& dst, vector<int64_t>::iterator begin, vector<int64_t>::iterator end):
            _dst(dst), _begin(begin), _end(end), _it(begin)
        {
            dst.seekp(0);
        }

        ~ChunkWriter()
        {
            flush();
        }

        void append(int64_t val)
        {
            *_it = val;
            ++_it;
            if(_it == _end)
            {
                flush();
                _it = _begin;
            }
        }
};

/**
 * @brief The FileWrapper struct
 */
struct FileWrapper
{
        filesystem::path path;
        fstream stream;

        FileWrapper(const filesystem::path& basePath, const char* postfix): path(basePath)
        {
            path += postfix;
            stream.open(path.string(), ios::binary | ios::trunc | fstream::out | fstream ::in);
            if(!stream.is_open())
                throw runtime_error(string("Can't open file ") + path.string());
            stream.exceptions(ifstream::badbit);
        }

        ~FileWrapper()
        {
            if(stream.is_open())
                stream.close();
            if(!path.empty())
                filesystem::remove(path);
        }

        void rename(const filesystem::path& newPath)
        {
            if(stream.is_open())
                stream.close();
            if(filesystem::exists(newPath))
                filesystem::remove(newPath);
            filesystem::rename(path, newPath);
            path.clear();
        }
};

/**
 * @brief presort - предварительная сортировка фрагментов исходного файла. Отсортированные фрагменты будут записаны в dst.
 * @param srcPath - название исходного файла с путём до него
 * @param dst - файловый поток, который будут записаны отсортированные фрагменты исходного файла
 * @param buf - предварительно выделенный массив, в который будут вычитываться фрагменты исходного файла
 */
void presort(const filesystem::path& srcPath, fstream& dst, vector<int64_t>& buf)
{
    ifstream src(srcPath.string(), ios::binary);
    if(!src.is_open())
        throw runtime_error(string("Can't open file ") + srcPath.string());
    src.exceptions(ifstream::badbit);
    while(!src.eof())
    {
        src.read(reinterpret_cast<char*>(buf.data()), buf.size() * sizeof(int64_t));
        size_t elCount = src.gcount() / sizeof(int64_t);
        sort(std::begin(buf), std::begin(buf) + elCount);
        dst.write(reinterpret_cast<char*>(buf.data()), elCount * sizeof(int64_t));
    }
    dst.flush();
}

/**
 * @brief Функция слияния двух фрагментов с записью результата через объект класса ChunkWriter
 * @param ch1 - первый фрагмент
 * @param ch2 - второй фрагмент
 * @param cw - объект, с помощью которого делается запись в файл
 */
void mergeSiblingChunks(Chunk& ch1, Chunk& ch2, ChunkWriter& cw)
{
    int64_t v1;
    int64_t v2;
    if(ch1.getNext(v1))
    {
        if(ch2.getNext(v2))
        {
            while(true)
            {
                if(v1 < v2)
                {
                    cw.append(v1);
                    if(!ch1.getNext(v1))
                    {
                        cw.append(v2);
                        break;
                    }
                }
                else
                {
                    cw.append(v2);
                    if(!ch2.getNext(v2))
                    {
                        cw.append(v1);
                        break;
                    }
                }
            }
        }
        else
        {
            cw.append(v1);
        }
    }
    while(ch1.getNext(v1))
        cw.append(v1);
    while(ch2.getNext(v2))
        cw.append(v2);
}

/**
 * @brief Слияние отсортированных фрагментов из файла src в более длинные отсортированные фрагменты и сохранение их в dst
 * @param src - поток файла с исходными отсоритрованными фрагментами
 * @param dst - поток файла в который будут сохранены слитые фрагменты
 * @param buf - предварительно выделенный буфер для хранения данных.
 *              2 трети используются как бефер для считанных частей фрагментов из исходного файла, одна треть - часть слитого фрагмента
 * @param numbersInChunk - максимальное количество чисел int64_t в фрагменте исходного файла
 * @param chunkCount - число фрагментов в исходном файле
 * @return numbersInChunk * 2 -  максимальное количество чисел в фрагменте в dst. Оно в 2 раза больше чем в src, т.к. сливаются соседние фрагменты
 */
size_t mergeChunks(fstream& src, fstream& dst, vector<int64_t>& buf, size_t numbersInChunk, size_t chunkCount)
{
    size_t l = buf.size() / 3;
    ChunkWriter cw(dst, buf.begin(), buf.begin() + l);
    for(size_t chunkIndex = 0; chunkIndex < chunkCount; chunkIndex += 2)
    {
        Chunk ch1(src, buf.begin() + l, l, numbersInChunk, chunkIndex);
        Chunk ch2(src, buf.begin() + 2 * l, l, (chunkIndex + 1 < chunkCount) ? numbersInChunk : 0, chunkIndex + 1);
        mergeSiblingChunks(ch1, ch2, cw);
    }
    return numbersInChunk * 2;
}

/**
 * @brief Отсортировать данные файла с 64-битными знаковыми целыми числами
 * @param srcPath - путь с именем исходного файла
 * @param dstPath - путь с именем файла, в котором будет сохранён результат сортировки
 * @param ramSizeInBytes - размер промежуточного буфера в памяти, используемого для сортировки
 */
void sortFile(const filesystem::path& srcPath, const filesystem::path& dstPath, size_t ramSizeInBytes)
{
    FileWrapper w[2] = { {dstPath, ".1"}, {dstPath, ".2"} };
    FileWrapper* pw[2] = { w, w + 1 };

    size_t numbersInChunk = ramSizeInBytes / sizeof(int64_t);
    vector<int64_t> buf(numbersInChunk);

    presort(srcPath, w[0].stream, buf);

    size_t fileSize = filesystem::file_size(srcPath);
    size_t numbersInFile = fileSize / sizeof(int64_t);

    while(numbersInChunk < numbersInFile)
    {
        size_t chunkCount = numbersInFile / numbersInChunk;
        if(numbersInFile % numbersInChunk)
            ++chunkCount;
        numbersInChunk = mergeChunks(pw[0]->stream, pw[1]->stream, buf, numbersInChunk, chunkCount);
        std::swap(pw[0], pw[1]);
    }
    pw[0]->rename(dstPath);
}

void generateTestData(const char* name, int64_t n)
{
    ofstream f(name, ios::binary | ios::trunc);
    if(!f.is_open())
        return;
    for(int64_t i = n; i > 0; --i)
        f.write(reinterpret_cast<char*>(&i), sizeof(i));
}

int main(int argc, char* argv[])
{
//    generateTestData("test1.bin", 21);
//    generateTestData("test2.bin", 20);
//    generateTestData("test3.bin", 6);
//    generateTestData("test4.bin", 0);
//    generateTestData("test5.bin", 1);


    if(argc != 4)
    {
        cout << "Sorting of a file with signed 64-bit integers." << endl <<"Usage: BigSort <source file> <destination file> <buffer size in bytes>" << endl;
        return 1;
    }

    size_t ramSize = std::stoull(argv[3]);
    const size_t minRamSize = sizeof(int64_t) * 3 * 2; // Пусть хоть по 2 числа в одновременно можно хранить в памяти
    if(ramSize < minRamSize)
    {
        cout << "Buffer size can't be less than " << minRamSize <<" bytes" << endl;
        return 1;
    }

    try
    {
        sortFile(filesystem::path(argv[1]), filesystem::path(argv[2]), ramSize);
    }
    catch (std::exception& ex)
    {
        cout << ex.what() << endl;
    }
    catch (...)
    {
        cout << "Unknown error" << endl;
    }

    return 0;
}
