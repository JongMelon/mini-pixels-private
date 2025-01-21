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

#include "writer/DecimalColumnWriter.h"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) :
ColumnWriter(type, writerOption)/*, curPixelVector(pixelStride)*/
{

}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{

    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);

    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }

    long* values = columnVector->vector;
    EncodingUtils encodingUtils;

    for (int i = 0; i < size; i++) {
        isNull[curPixelIsNullIndex] = columnVector->isNull[i];
        curPixelEleIndex++;

        if (columnVector->isNull[i]) {
            hasNull = true;
            encodingUtils.writeLongLE(outputStream, 0L);
        }
        else {
            if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
                encodingUtils.writeLongLE(outputStream, values[i]);
            }
            else {
                encodingUtils.writeLongBE(outputStream, values[i]);
            }
        }

        if (curPixelEleIndex >= pixelStride) {
            newPixel();
        }
    }
    return outputStream->getWritePos();
}

/*void DecimalColumnWriter::close()
{
    if (runlengthEncoding && encoder)
    {
        encoder->clear();
    }
    ColumnWriter::close();
}*/

/*void DecimalColumnWriter::writeCurPartTime(std::shared_ptr<ColumnVector> columnVector, long *values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset])
        {
            hasNull = true;
            if (nullsPadding)
            {
                // padding 0 for nulls
                curPixelVector[curPixelVectorIndex++] = 0L;
            }
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
            std::cout << values[i + curPartOffset] << std::endl;
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}*/

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    /*if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }*/
    return writerOption->isNullsPadding();
}

/*void DecimalColumnWriter::newPixel()
{
    // write out current pixel vector
    if (runlengthEncoding)
    {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(long));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    }
    else
    {
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(long));
        if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
                double d = 0;
                memcpy(&d, &curPixelVector[i], sizeof(d));
                std::cout << d << std::endl;
            }
        }
        else
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                encodingUtils.writeLongBE(curVecPartitionBuffer, curPixelVector[i]);
                double d = 0;
                memcpy(&d, &curPixelVector[i], sizeof(d));
                std::cout << d << std::endl;
            }
        }
        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    //}

    ColumnWriter::newPixel();
}*/
/*
pixels::proto::ColumnEncoding DecimalColumnWriter::getColumnChunkEncoding() const
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding)
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}
*/