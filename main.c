#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#define COLUMNS 25
#define BUFFER_SIZE 100
// #define FILE_NAME "pubg-data.csv"

int **matrix; // 2-d array - dynamic

int data_per_thread;

void *thread_routine(void *args)
{
    int start = *((int *)args);
    int end = start + data_per_thread;
    // printf("Thread is called - %u\n", pthread_self());
    int max = 0;
    for (size_t i = start; i < end; i++)
    {
        // printf("%d\n", matrix[i][22]); // walkDistance
        if (matrix[i][22] > max)
            max = matrix[i][22];
    }

    printf("%d to %d\n", start, end);
    printf("Max from Thread ID: %d\n", max);
    int *temp = (int *)malloc(sizeof(int));
    *temp = max;
    return (void *)temp;
}



int main(int argc, char const *argv[])
{
    char *file_name = "pubg-data.csv";

    FILE *fp;

    fp = fopen(file_name, "r"); 

    char column_names[1000];
    fscanf(fp, "%s\n", column_names); // read columns header
    // printf("%s\n", column_names);


    int fd[2];

    pipe(fd);

    if (fork() == 0) // child
    {
        close(fd[0]); // close read end
        dup2(fd[1], 1); // to stdout[1] - fd[1] = write
        close(fd[1]);

        execlp("wc", "wc", "-l", file_name, NULL);
    }

    char buffer[BUFFER_SIZE];
    close(fd[1]); // close write end
    read(fd[0], buffer, BUFFER_SIZE);
    int n_lines = atoi(buffer); // char[] to int
    close(fd[0]);

    // printf("%d\n", n_lines);

    data_per_thread = n_lines / 10;
    // if (n_lines > 1000)
    // {
    //     data_per_thread = 100;
    // }
    // else if (n_lines > 10000)
    // {
    //     data_per_thread = 2000;
    // }

    int n_threads = n_lines / data_per_thread;


    matrix = (int **)malloc(sizeof(int *) * n_lines);

    pthread_t *ptr_tid;

    ptr_tid = (pthread_t *)malloc(sizeof(pthread_t) * n_threads);


    int *temp = (int *)malloc(sizeof(int) * n_threads);

    for (size_t i = 0; i < n_lines; i++)
    {
        matrix[i] = (int *)malloc(sizeof(int) * COLUMNS);
    }
    // data_per_thread = n_lines / n_threads;
    int i = 0;
    int thread_count = 0;
    
    while (!feof(fp))
    {
        // printf("Kushal - %d\n",i);
        // fscanf(fp, "%s\n", column_names); // read columns header
        fscanf(fp, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", &matrix[i][0], &matrix[i][1], &matrix[i][2], &matrix[i][3], &matrix[i][4], &matrix[i][5], &matrix[i][6], &matrix[i][7], &matrix[i][8], &matrix[i][9], &matrix[i][10], &matrix[i][11], &matrix[i][12], &matrix[i][13], &matrix[i][14], &matrix[i][15], &matrix[i][16], &matrix[i][17], &matrix[i][18], &matrix[i][19], &matrix[i][20], &matrix[i][21], &matrix[i][22], &matrix[i][23], &matrix[i][24]);

        // fscanf(fp, "%d,%d,%d", &matrix[i][0], &matrix[i][1], &matrix[i][2]);
        // printf("%d,%d,%d,%d\n", matrix[i][0], matrix[i][1], matrix[i][2], matrix[i][24]);

        i += 1;
        // printf("%d\n",i);

        if ((i % data_per_thread) == 0 || (i + 1) == n_lines)
        {
            if (i == (n_lines - 1))
                i++;
            temp[thread_count] = i;
            pthread_create(&ptr_tid[thread_count++], NULL, thread_routine, (void *)&temp[thread_count - 1]);
        }
    }

    // printf("Thread Count - %d\n", thread_count);
    int *ret, max = 0;
    for (size_t i = 0; i < thread_count; i++)
    {
        pthread_join(ptr_tid[i], (void *)&ret);
        if (max < *ret)
            max = *ret;
        free(ret);
    }
    printf("Max - %d\n", max);


    //free up memory
    free(temp);
    free(ptr_tid);

    for (size_t i = 0; i < n_lines; i++)
        free(matrix[i]);
    free(matrix);

    return 0;
}
