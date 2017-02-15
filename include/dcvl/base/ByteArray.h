/**
 * licensed to the apache software foundation (asf) under one
 * or more contributor license agreements.  see the notice file
 * distributed with this work for additional information
 * regarding copyright ownership.  the asf licenses this file
 * to you under the apache license, version 2.0 (the
 * "license"); you may not use this file except in compliance
 * with the license.  you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

#pragma once

#include <cstring>
#include <vector>
#include <cstdint>

namespace dcvl {
    namespace base {
        class ByteArray : public std::vector<char> {
        public:
            ByteArray() = default;

            ByteArray(size_t size) :
                std::vector<char>(size) {
            }

            ByteArray(const char* buffer, int32_t size) :
                std::vector<char>(buffer, buffer + size) {
            }

            std::string ToStdString() const {
                std::string result(this->cbegin(), this->cend());

                return result;
            }

            ByteArray& Concat(const ByteArray& buffer2) {
                size_t oldSize = size();
                size_t newSize = oldSize + buffer2.size();
                resize(newSize);
                memcpy(this->data() + oldSize, buffer2.data(), buffer2.size());

                return *this;
            }

            ByteArray operator+(const ByteArray& buffer2) const {
                ByteArray buffer1(this->size() + buffer2.size());
                memcpy(buffer1.data(), this->data(), this->size());
                memcpy(buffer1.data() + this->size(), buffer2.data(), buffer2.size());

                return buffer1;
            }
        };

        class IODevice {
        public:
            enum class SeekMode {
                Set,
                Forward,
                Backward
            };

            ~IODevice() {}
        };

        class ByteArrayReader : public IODevice {
        public:
            ByteArrayReader(const ByteArray& buffer) :
                _buffer(buffer), _pos(0) {}

            template <class T>
            int32_t Read(T* buffer, int32_t count) {
                if ( _pos >= static_cast<int32_t>(_buffer.size()) ) {
                    return 0;
                }

                size_t sizeToRead = sizeof(T) * count;
                if ( _pos + sizeToRead > static_cast<int32_t>(_buffer.size()) ) {
                    sizeToRead = _buffer.size() - _pos;
                }

                memcpy(buffer, _pos + _buffer.data(), sizeToRead);
                _pos += static_cast<int32_t>(sizeToRead);

                return static_cast<int32_t>(sizeToRead);
            }

            template <class T>
            T Read() {
                T t;
                Read(&t, 1);

                return t;
            }

            ByteArray ReadData(int32_t size) {
                if ( _pos >= static_cast<int32_t>(_buffer.size()) ) {
                    return 0;
                }

                size_t sizeToRead = size;
                if ( _pos + sizeToRead > static_cast<int32_t>(_buffer.size()) ) {
                    sizeToRead = _buffer.size() - static_cast<size_t>(_pos);
                }

                ByteArray result(sizeToRead);
                memcpy(result.data(), _buffer.data() + _pos, sizeToRead);
                _pos += static_cast<int32_t>(sizeToRead);

                return result;
            }

            int32_t Tell() const {
                return _pos;
            }

            void Seek(SeekMode mode, int32_t size) {
                int32_t dest = _pos;
                if ( mode == SeekMode::Set ) {
                    dest = size;
                }
                else if ( mode == SeekMode::Forward ) {
                    dest += size;
                }
                else if ( mode == SeekMode::Backward ) {
                    dest -= size;
                }
            }
        private:
            ByteArray _buffer;
            int32_t _pos;
        };

        class ByteArrayWriter {
        public:
            ByteArrayWriter() {
            }

            template <class T>
            int32_t Write(const T* buffer, int32_t count) {
                int32_t sizeToWrite = sizeof(T) * count;
                ByteArray buffer2((const char*)(buffer), sizeToWrite);
                _buffer.Concat(buffer2);

                return sizeToWrite;
            }

            template <class T>
            int32_t Write(const T& value) {
                return Write(&value, 1);
            }

            int32_t Write(const ByteArray& buffer) {
                _buffer.Concat(buffer);

                return static_cast<int32_t>(buffer.size());
            }

            const ByteArray& ToByteArray() const {
                return _buffer;
            }


            int32_t Tell() const {
                return static_cast<int32_t>(_buffer.size());
            }

        private:
            ByteArray _buffer;
        };

    }
}
