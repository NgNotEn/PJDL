// package util;

// import java.util.Random;
// import java.util.concurrent.*;

// import joining.JoinProcessor;

// public class QuickSort {
//     // 使用您提供的线程池
//     public static final ExecutorService executorService = JoinProcessor.sortExecutorService;
    
//     // 排序阈值常量
//     private static final int PARALLEL_THRESHOLD = 20000; // 启用并行排序的阈值
//     private static final int SEQUENTIAL_THRESHOLD = 8000; // 并行任务切换到串行的阈值
//     private static final int INSERTION_THRESHOLD = 16; // 切换到插入排序的阈值
//     private static final int MAX_RECURSION_DEPTH = 64; // 最大递归深度限制
//     private static final int MAX_PARALLEL_DEPTH = 4; // 最大并行深度

//     private static final Random random = new Random();

//     /**
//      * 对指定范围的索引进行排序
//      * 
//      * @param low     起始索引
//      * @param high    结束索引
//      * @param indices 索引数组
//      * @param values  对应的值数组
//      */
//     public static void sort(int low, int high, int[] indices, int[] values) {
//         if (high - low > PARALLEL_THRESHOLD) {
//             // 数组足够大，使用并行排序
//             parallelQuickSort(low, high, indices, values, 0);
//         } else {
//             // 使用串行排序
//             sequentialQuickSort(low, high, indices, values);
//         }
//     }

//     /**
//      * 带线程数控制的排序方法
//      * 
//      * @param low        起始索引
//      * @param high       结束索引
//      * @param indices    索引数组
//      * @param values     对应的值数组
//      * @param maxThreads 最大线程数
//      */
//     public static void sort(int low, int high, int[] indices, int[] values, int maxThreads) {
//         if (high - low > PARALLEL_THRESHOLD && maxThreads > 1) {
//             // 使用自定义线程池
//             parallelQuickSort(low, high, indices, values, 0);
//         } else {
//             // 使用串行排序
//             sequentialQuickSort(low, high, indices, values);
//         }
//     }

//     /**
//      * 串行快速排序实现（迭代方式）
//      */
//     private static void sequentialQuickSort(int low, int high, int[] indices, int[] values) {
//         // 使用迭代方法避免栈溢出
//         int[] stack = new int[(high - low + 1) * 2];
//         int[] depthStack = new int[high - low + 1];
//         int top = -1;
//         int depthTop = -1;

//         // 将初始范围压入栈
//         stack[++top] = low;
//         stack[++top] = high;
//         depthStack[++depthTop] = 0;

//         while (top >= 0) {
//             // 弹出high、low和depth
//             high = stack[top--];
//             low = stack[top--];
//             int depth = depthStack[depthTop--];

//             if (low < high) {
//                 // 如果递归深度过深，切换到堆排序
//                 if (depth > MAX_RECURSION_DEPTH) {
//                     heapSort(low, high, indices, values);
//                     continue;
//                 }

//                 // 对小数组使用插入排序
//                 if (high - low < INSERTION_THRESHOLD) {
//                     insertionSortIndices(low, high, indices, values);
//                     continue;
//                 }

//                 int pivotIndex = partitionWithRandomPivot(low, high, indices, values);

//                 // 将左子数组范围压入栈
//                 if (pivotIndex - 1 > low) {
//                     stack[++top] = low;
//                     stack[++top] = pivotIndex - 1;
//                     depthStack[++depthTop] = depth + 1;
//                 }

//                 // 将右子数组范围压入栈
//                 if (pivotIndex + 1 < high) {
//                     stack[++top] = pivotIndex + 1;
//                     stack[++top] = high;
//                     depthStack[++depthTop] = depth + 1;
//                 }
//             }
//         }
//     }

//     /**
//      * 使用随机枢轴的分区函数
//      */
//     private static int partitionWithRandomPivot(int low, int high, int[] indices, int[] values) {
//         // 随机选择枢轴以避免最坏情况
//         int randomIndex = low + random.nextInt(high - low + 1);
//         swap(indices, randomIndex, high);

//         return partition(low, high, indices, values);
//     }

//     /**
//      * 堆排序实现（当递归深度过深时使用）
//      */
//     private static void heapSort(int low, int high, int[] indices, int[] values) {
//         int n = high - low + 1;

//         // 构建最大堆
//         for (int i = n / 2 - 1; i >= 0; i--) {
//             heapify(indices, values, low, n, i);
//         }

//         // 逐个提取元素
//         for (int i = n - 1; i > 0; i--) {
//             swap(indices, low, low + i);
//             heapify(indices, values, low, i, 0);
//         }
//     }

//     private static void heapify(int[] indices, int[] values, int offset, int n, int i) {
//         int largest = i;
//         int left = 2 * i + 1;
//         int right = 2 * i + 2;

//         if (left < n && values[indices[offset + left]] > values[indices[offset + largest]]) {
//             largest = left;
//         }

//         if (right < n && values[indices[offset + right]] > values[indices[offset + largest]]) {
//             largest = right;
//         }

//         if (largest != i) {
//             swap(indices, offset + i, offset + largest);
//             heapify(indices, values, offset, n, largest);
//         }
//     }

//     /**
//      * 分区函数
//      * 
//      * @return 枢轴位置
//      */
//     private static int partition(int low, int high, int[] indices, int[] values) {
//         // 使用三点中值法选择枢轴
//         int mid = low + ((high - low) >>> 1);
//         if (values[indices[mid]] < values[indices[low]]) {
//             swap(indices, low, mid);
//         }
//         if (values[indices[high]] < values[indices[low]]) {
//             swap(indices, low, high);
//         }
//         if (values[indices[high]] < values[indices[mid]]) {
//             swap(indices, mid, high);
//         }

//         int pivot = values[indices[high]];
//         int i = low - 1;

//         for (int j = low; j < high; j++) {
//             if (values[indices[j]] <= pivot) {
//                 i++;
//                 swap(indices, i, j);
//             }
//         }

//         swap(indices, i + 1, high);
//         return i + 1;
//     }

//     /**
//      * 交换数组中的两个元素
//      */
//     private static void swap(int[] arr, int i, int j) {
//         int temp = arr[i];
//         arr[i] = arr[j];
//         arr[j] = temp;
//     }

//     /**
//      * 并行快速排序实现（使用自定义线程池）
//      */
//     private static void parallelQuickSort(int low, int high, int[] indices, int[] values, int depth) {
//         if (low >= high) return;
        
//         // 深度控制：超过最大深度或数组太小则使用串行
//         if (depth > MAX_PARALLEL_DEPTH || high - low < SEQUENTIAL_THRESHOLD) {
//             sequentialQuickSort(low, high, indices, values);
//             return;
//         }
        
//         // 执行分区操作
//         int pivotIndex = partitionWithRandomPivot(low, high, indices, values);
        
//         // 创建左右子数组的排序任务
//         CompletableFuture<Void> leftFuture = null;
//         CompletableFuture<Void> rightFuture = null;
        
//         if (pivotIndex - 1 > low) {
//             final int finalLow = low;
//             final int finalHigh = pivotIndex - 1;
//             leftFuture = CompletableFuture.runAsync(() -> {
//                 parallelQuickSort(finalLow, finalHigh, indices, values, depth + 1);
//             }, executorService);
//         }
        
//         if (pivotIndex + 1 < high) {
//             final int finalLow = pivotIndex + 1;
//             final int finalHigh = high;
//             rightFuture = CompletableFuture.runAsync(() -> {
//                 parallelQuickSort(finalLow, finalHigh, indices, values, depth + 1);
//             }, executorService);
//         }
        
//         // 等待任务完成
//         try {
//             if (leftFuture != null && rightFuture != null) {
//                 CompletableFuture.allOf(leftFuture, rightFuture).get();
//             } else if (leftFuture != null) {
//                 leftFuture.get();
//             } else if (rightFuture != null) {
//                 rightFuture.get();
//             }
//         } catch (InterruptedException | ExecutionException e) {
//             Thread.currentThread().interrupt();
//             // 并行失败时回退到串行
//             sequentialQuickSort(low, high, indices, values);
//         }
//     }

//     private static void insertionSortIndices(int low, int high, int[] indices, int[] values) {
//         for (int i = low + 1; i <= high; i++) {
//             int key = indices[i];
//             int keyValue = values[key];
//             int j = i - 1;

//             while (j >= low && values[indices[j]] > keyValue) {
//                 indices[j + 1] = indices[j];
//                 j--;
//             }
//             indices[j + 1] = key;
//         }
//     }

// }

























package util;

import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class QuickSort {

    // 排序阈值常量
    private static final int PARALLEL_THRESHOLD = 20000; // 启用并行排序的阈值
    private static final int SEQUENTIAL_THRESHOLD = 8000; // 并行任务切换到串行的阈值
    private static final int INSERTION_THRESHOLD = 16; // 切换到插入排序的阈值
    private static final int MAX_RECURSION_DEPTH = 64; // 最大递归深度限制

    // 线程控制常量
    private static final int MAX_THREADS = 64; 

    private static final Random random = new Random();

    // 线程池实例，使用指定的线程数
    private static final ForkJoinPool customThreadPool = new ForkJoinPool(MAX_THREADS);

    /**
     * 对指定范围的索引进行排序
     * 
     * @param low     起始索引
     * @param high    结束索引
     * @param indices 索引数组
     * @param values  对应的值数组
     */
    public static void sort(int low, int high, int[] indices, int[] values) {
        if (high - low > PARALLEL_THRESHOLD) {
            // 数组足够大，使用并行排序
            customThreadPool.invoke(new ParallelQuickSort(low, high, indices, values, 0));
        } else {
            // 使用串行排序
            sequentialQuickSort(low, high, indices, values);
        }
    }

    /**
     * 带线程数控制的排序方法
     * 
     * @param low        起始索引
     * @param high       结束索引
     * @param indices    索引数组
     * @param values     对应的值数组
     * @param maxThreads 最大线程数
     */
    public static void sort(int low, int high, int[] indices, int[] values, int maxThreads) {
        if (high - low > PARALLEL_THRESHOLD && maxThreads > 1) {
            // 创建指定线程数的线程池
            ForkJoinPool pool = new ForkJoinPool(maxThreads);
            try {
                pool.invoke(new ParallelQuickSort(low, high, indices, values, 0));
            } finally {
                pool.shutdown();
            }
        } else {
            // 使用串行排序
            sequentialQuickSort(low, high, indices, values);
        }
    }

    /**
     * 串行快速排序实现（迭代方式）
     */
    private static void sequentialQuickSort(int low, int high, int[] indices, int[] values) {
        // 使用迭代方法避免栈溢出
        int[] stack = new int[(high - low + 1) * 2];
        int[] depthStack = new int[high - low + 1];
        int top = -1;
        int depthTop = -1;

        // 将初始范围压入栈
        stack[++top] = low;
        stack[++top] = high;
        depthStack[++depthTop] = 0;

        while (top >= 0) {
            // 弹出high、low和depth
            high = stack[top--];
            low = stack[top--];
            int depth = depthStack[depthTop--];

            if (low < high) {
                // 如果递归深度过深，切换到堆排序
                if (depth > MAX_RECURSION_DEPTH) {
                    heapSort(low, high, indices, values);
                    continue;
                }

                // 对小数组使用插入排序
                if (high - low < INSERTION_THRESHOLD) {
                    insertionSortIndices(low, high, indices, values);
                    continue;
                }

                int pivotIndex = partitionWithRandomPivot(low, high, indices, values);

                // 将左子数组范围压入栈
                if (pivotIndex - 1 > low) {
                    stack[++top] = low;
                    stack[++top] = pivotIndex - 1;
                    depthStack[++depthTop] = depth + 1;
                }

                // 将右子数组范围压入栈
                if (pivotIndex + 1 < high) {
                    stack[++top] = pivotIndex + 1;
                    stack[++top] = high;
                    depthStack[++depthTop] = depth + 1;
                }
            }
        }
    }

    /**
     * 使用随机枢轴的分区函数
     */
    private static int partitionWithRandomPivot(int low, int high, int[] indices, int[] values) {
        // 随机选择枢轴以避免最坏情况
        int randomIndex = low + random.nextInt(high - low + 1);
        swap(indices, randomIndex, high);

        return partition(low, high, indices, values);
    }

    /**
     * 堆排序实现（当递归深度过深时使用）
     */
    private static void heapSort(int low, int high, int[] indices, int[] values) {
        int n = high - low + 1;

        // 构建最大堆
        for (int i = n / 2 - 1; i >= 0; i--) {
            heapify(indices, values, low, n, i);
        }

        // 逐个提取元素
        for (int i = n - 1; i > 0; i--) {
            swap(indices, low, low + i);
            heapify(indices, values, low, i, 0);
        }
    }

    private static void heapify(int[] indices, int[] values, int offset, int n, int i) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < n && values[indices[offset + left]] > values[indices[offset + largest]]) {
            largest = left;
        }

        if (right < n && values[indices[offset + right]] > values[indices[offset + largest]]) {
            largest = right;
        }

        if (largest != i) {
            swap(indices, offset + i, offset + largest);
            heapify(indices, values, offset, n, largest);
        }
    }

    /**
     * 分区函数
     * 
     * @return 枢轴位置
     */
    private static int partition(int low, int high, int[] indices, int[] values) {
        // 使用三点中值法选择枢轴
        int mid = low + ((high - low) >>> 1);
        if (values[indices[mid]] < values[indices[low]]) {
            swap(indices, low, mid);
        }
        if (values[indices[high]] < values[indices[low]]) {
            swap(indices, low, high);
        }
        if (values[indices[high]] < values[indices[mid]]) {
            swap(indices, mid, high);
        }

        int pivot = values[indices[high]];
        int i = low - 1;

        for (int j = low; j < high; j++) {
            if (values[indices[j]] <= pivot) {
                i++;
                swap(indices, i, j);
            }
        }

        swap(indices, i + 1, high);
        return i + 1;
    }

    /**
     * 交换数组中的两个元素
     */
    private static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    /**
     * 并行快速排序实现类
     */
    private static class ParallelQuickSort extends RecursiveAction {
        private final int low;
        private final int high;
        private final int[] indices;
        private final int[] values;
        private final int depth;

        public ParallelQuickSort(int low, int high, int[] indices, int[] values, int depth) {
            this.low = low;
            this.high = high;
            this.indices = indices;
            this.values = values;
            this.depth = depth;
        }

        @Override
        protected void compute() {
            // 如果递归深度过深，切换到串行排序
            if (depth > MAX_RECURSION_DEPTH || high - low < SEQUENTIAL_THRESHOLD) {
                sequentialQuickSort(low, high, indices, values);
                return;
            }

            if (low >= high) {
                return;
            }

            // 执行分区操作
            int pivotIndex = partitionWithRandomPivot(low, high, indices, values);

            // 创建并行任务处理左右两部分
            ParallelQuickSort leftTask = null;
            ParallelQuickSort rightTask = null;

            if (pivotIndex - 1 > low) {
                leftTask = new ParallelQuickSort(low, pivotIndex - 1, indices, values, depth + 1);
            }

            if (pivotIndex + 1 < high) {
                rightTask = new ParallelQuickSort(pivotIndex + 1, high, indices, values, depth + 1);
            }

            // 执行任务
            if (leftTask != null && rightTask != null) {
                invokeAll(leftTask, rightTask);
            } else if (leftTask != null) {
                leftTask.compute();
            } else if (rightTask != null) {
                rightTask.compute();
            }
        }
    }

    private static void insertionSortIndices(int low, int high, int[] indices, int[] values) {
        for (int i = low + 1; i <= high; i++) {
            int key = indices[i];
            int keyValue = values[key];
            int j = i - 1;

            while (j >= low && values[indices[j]] > keyValue) {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = key;
        }
    }

    // /**
    //  * 关闭自定义线程池（在应用程序结束时调用）
    //  */
    // public static void shutdown() {
    //     customThreadPool.shutdown();
    // }
}