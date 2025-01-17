/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/StringColumnWriter.h"

StringColumnWriter::StringColumnWriter(std::shared_ptr<TypeDescription> type,std::shared_ptr<PixelsWriterOption> writerOption):
ColumnWriter(type,writerOption), curPixelVector(pixelStride) {
    encodingUtils = std::make_shared<EncodingUtils>();
    /*runlengthEncoding = false; //encodingLevel.ge(EncodingLevel::Level::EL2);
    dictionaryEncoding = false;
    this->writerOption = writerOption;
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>();
    }*/
    startsArray=std::make_shared<DynamicIntArray>();
}

int StringColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length) {
    std::cout << "StringColumnWriter::write" << std::endl;

    auto columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);

    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }

    auto values = columnVector->str_vec;
    EncodingUtils encodingUtils;

    //int offset = 0;

    for (int i = 0; i < length; i++) {
        isNull[curPixelIsNullIndex] = columnVector->isNull[i];
        curPixelEleIndex++;

        if (columnVector->isNull[i]) {
            hasNull = true;
            if (nullsPadding) {
                //outputStream->put(0);
                std::cout << "Should not reach here" << std::endl;
            }
        }
        else {
            int str_size = values[i].size();
            outputStream->putBytes((u_int8_t*)values[i].c_str(), str_size, startOffset);
            startsArray->add(startOffset);
            startOffset += str_size;
            std::cout << "Str, len and offset: " << values[i] << " " << str_size << " " << startOffset << std::endl;
        }

        if (curPixelEleIndex >= pixelStride) {
            newPixel();
        }
    }
    return outputStream->getWritePos();

    /*auto columnVector = std::dynamic_pointer_cast<BinaryColumnVector>(vector);
    duckdb::string_t* values = columnVector->vector;

    auto str_vec = columnVector->str_vec;

    std::vector<int> vLens(length);
    std::vector<int> vOffsets(length);
    
    int curOffset = 0;
    
    for (int i = 0; i < length; i++) {
        vLens[i] = str_vec[i].size();  // Store the length of the string
        vOffsets[i] = curOffset;        // Store the current offset
        curOffset += vLens[i];          // Update the offset
    }

    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = length;

    /*if (dictionaryEncoding) {
        // Process dictionary encoding case
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartWithDict(columnVector, values, vLens.data(), vOffsets.data(), curPartLength, curPartOffset);
            newPixels();
            curPartOffset += curPartLength;
            nextPartLength = length - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartWithDict(columnVector, values, vLens.data(), vOffsets.data(), curPartLength, curPartOffset);
    }
    else {
        // Process without dictionary encoding
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartWithoutDict(writerOption, str_vec, vLens.data(), vOffsets.data(), curPartLength, curPartOffset);
            newPixels();
            curPartOffset += curPartLength;
            nextPartLength = length - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartWithoutDict(writerOption, str_vec, vLens.data(), vOffsets.data(), curPartLength, curPartOffset);
    //}
    return outputStream->getWritePos();*/
}

void StringColumnWriter::newPixels() {
    /*if (runlengthEncoding) {
        u_int8_t* temp_buffer = new u_int8_t[curPixelVectorIndex * 8];
        int encode_length = 0;
        encoder->encode(curPixelVector.data(), 0, curPixelVectorIndex, temp_buffer, encode_length);
        if (encode_length > curPixelVectorIndex * 8) {
            std::cout << "StringColumnWriter::newPixels: encode_length > curPixelVectorIndex * 8" << std::endl;
        }
        outputStream->putBytes(temp_buffer, encode_length);
        delete[] temp_buffer;
    }
    else {
        std::cout << "Should not reach here" << std::endl;
    }*/
    /*else if (dictionaryEncoding) {
        /*if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
            for (int i = 0; i < curPixelVectorIndex; ++i) {
                encodingUtils->writeIntLE(outputStream, curPixelVector[i]);
            }
        } else {
            for (int i = 0; i < curPixelVectorIndex; ++i) {
                encodingUtils->writeIntBE(outputStream, curPixelVector[i]);
            }
        }*/
    //}
    ColumnWriter::newPixel();
}

void StringColumnWriter::writeCurPartWithoutDict(std::shared_ptr<PixelsWriterOption> writerOption, 
                                                std::vector<std::string>& values, int* vLens, int* vOffsets, 
                                                int curPartLength, int curPartOffset) {
    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (isNull[curPartOffset + i]) {
            hasNull = true;
            std::cout << "StringColumnWriter::writeCurPartWithoutDict: hasNull" << std::endl;
            //pixelStatRecorder.increment();
            if (nullsPadding) {
                // Padding with zero for null values
                startsArray->add(startOffset);
            }
        } else {
            // Write the actual data
            u_int8_t* temp_buffer = new u_int8_t[vLens[curPartOffset + i]];
            std::memcpy(temp_buffer, values[curPartOffset + i].c_str(), vLens[curPartOffset + i]);
            std::cout << "At index " << curPartOffset + i << " Put string: " << (char* )temp_buffer << std::endl;
            std::cout << "len and offset: " << vLens[curPartOffset + i] << " " << vOffsets[curPartOffset + i] << std::endl;
            outputStream->putBytes(temp_buffer, vLens[curPartOffset + i], vOffsets[curPartOffset + i]);
            startsArray->add(startOffset);
            std::cout << "StringColumnWriter::writeCurPartWithoutDict: startOffset = " << startOffset << std::endl;
            startOffset += vLens[curPartOffset + i];
            //pixelStatRecorder.updateString(values[curPartOffset + i].GetString(), 1);
            delete[] temp_buffer;
        }
    }
    // std::memcpy(isNull + curPixelIsNullIndex, isNull + curPartOffset, curPartLength);
    // curPixelIsNullIndex += curPartLength;
}

/*void StringColumnWriter::writeCurPartWithDict(std::shared_ptr<BinaryColumnVector> columnVector, 
                                              duckdb::string_t* values, int* vLens, int* vOffsets, 
                                              int curPartLength, int curPartOffset) {
    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding) {
                curPixelVector[curPixelVectorIndex++] = 0; // padding 0 for null values
            }
        } else {
            curPixelVector[curPixelVectorIndex++] = dictionary.add(values[curPartOffset + i].GetData(), 
                                                                  vOffsets[curPartOffset + i], 
                                                                  vLens[curPartOffset + i]);
            pixelStatRecorder.updateString(values[curPartOffset + i].GetData(), vOffsets[curPartOffset + i], 
                                           vLens[curPartOffset + i], 1);
        }
    }
    std::memcpy(isNull + curPixelIsNullIndex, columnVector->isNull + curPartOffset, curPartLength);
    curPixelIsNullIndex += curPartLength;
}*/

/*pixels::proto::ColumnEncoding StringColumnWriter::getColumnChunkEncoding() {
    if (dictionaryEncoding) {
        pixels::proto::ColumnEncoding builder = pixels::proto::ColumnEncoding()
            .setKind(pixels::proto::ColumnEncoding::Kind::DICTIONARY)
            .setDictionarySize(dictionary.size());
        if (runlengthEncoding) {
            builder.setCascadeEncoding(pixels::proto::ColumnEncoding()
                .setKind(pixels::proto::ColumnEncoding::Kind::RUNLENGTH));
        }
        return builder;
    }
    return pixels::proto::ColumnEncoding().setKind(pixels::proto::ColumnEncoding::Kind::NONE);
}*/

void StringColumnWriter::flush(){
    ColumnWriter::flush();
    flushStarts();
}

void StringColumnWriter::flushStarts() {
    //int startsFieldOffset=outputStream->size();
    int startsFieldOffset = startOffset;
    startsArray->add(startOffset);
    if(byteOrder==ByteOrder::PIXELS_LITTLE_ENDIAN) {
        for (int i=0;i<startsArray->size();i++) {
            encodingUtils->writeIntLE(outputStream, startsArray->get(i));
            std::cout << "StringColumnWriter::flushStarts: startOffset = " << startsArray->get(i) << std::endl;
        }
    }
    else {
        for(int i=0;i<startsArray->size();i++) {
            encodingUtils->writeIntBE(outputStream, startsArray->get(i));
        }
    }
    startsArray->clear();
    std::shared_ptr<ByteBuffer> offsetBuffer=std::make_shared<ByteBuffer>(4);
    offsetBuffer->putInt(startsFieldOffset);
    //std::cout << "StringColumnWriter::flushStarts: startsFieldOffset = " << startsFieldOffset << std::endl;
    //std::cout << "Bytes remaining " << outputStream->bytesRemaining() << std::endl;
    outputStream->putBytes(offsetBuffer->getPointer(), offsetBuffer->getWritePos());
}

bool StringColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    return writerOption->isNullsPadding();
}

void StringColumnWriter::close() {
    ColumnWriter::close();
}
