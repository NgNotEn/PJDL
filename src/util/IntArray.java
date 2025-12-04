package util;

public class IntArray {

    public int[] data;

    public int size;

    public final int maxCapacity;

    public IntArray() {
        maxCapacity = Integer.MAX_VALUE;
        data = new int[0];
    }

    public IntArray(int[] data, int maxCapacity) {
        this.data = data;
        this.size = data.length;
        this.maxCapacity = maxCapacity;
    }

    public IntArray(int initCapacity, int maxCapacity) {
        if (initCapacity <= 1)
            initCapacity = 2;
        data = new int[Math.min(initCapacity, maxCapacity)];
        size = 0;
        this.maxCapacity = maxCapacity;
    }

    public void add(int value) {
        if (size + 1 > data.length) {
            int newCapacity = Math.min(data.length * 2, maxCapacity);
            int[] newArray = new int[newCapacity];
            System.arraycopy(data, 0, newArray, 0, size);
            data = newArray;
        }
        data[size] = value;
        size++;
    }

    public void add(int value, int cnt) {
        int newSize = size + cnt;
        if (newSize > data.length) {
            int newCapacity = Math.max(newSize, Math.min(data.length * 2, maxCapacity));
            newCapacity = Math.min(newCapacity, maxCapacity);
            int[] newArray = new int[newCapacity];
            System.arraycopy(data, 0, newArray, 0, size);
            data = newArray;
        }

        for (int i = 0; i < cnt; i++)
            data[size + i] = value;
        size = newSize;
    }

    public void rangeSetValue(int lowerBound, int upperBound, int value) {
        if (upperBound > data.length)
            data = new int[Math.min(upperBound, maxCapacity)];
        this.size = upperBound;
        java.util.Arrays.fill(data, lowerBound, upperBound, value);
    }

    public void reSize(int size) {
        if (size > data.length)
            data = new int[Math.min(size, maxCapacity)];
        this.size = 0;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < size; i++) {
            sb.append(data[i]);
            if (i < size - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
