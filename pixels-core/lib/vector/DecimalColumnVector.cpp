//
// Created by yuly on 05.04.23.
//

#include <algorithm>
#include <cstring>
#include <cmath>
#include "vector/DecimalColumnVector.h"

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding): ColumnVector(len, encoding) {
	// decimal column vector has no encoding so we don't allocate memory to this->vector
	//this->vector = nullptr;
    posix_memalign(reinterpret_cast<void **>(&vector), 32,
                   len * sizeof(long));
    this->precision = precision;
    this->scale = scale;
    memoryUsage += (uint64_t) sizeof(uint64_t) * len;
}

void DecimalColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
		vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}

void DecimalColumnVector::add(std::string &value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    size_t pos = value.find('.');
    long integerValue = std::stoll(value.substr(0, pos) + value.substr(pos + 1));
    long l = integerValue * std::pow(10, scale - (value.length() - pos - 1));
    add(l);
}

void DecimalColumnVector::add(long value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    long *oldVector = vector;
    posix_memalign(reinterpret_cast<void **>(&vector), 32,
                    size * sizeof(long));
    if (preserveData) {
        std::copy(oldVector, oldVector + length, vector);
    }
    delete[] oldVector;
    memoryUsage += (long) sizeof(int) * (size - length);
    resize(size);
}
