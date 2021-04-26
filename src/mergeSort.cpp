/*

Problem Statement:

Parallel merge sort starts with n/comm_size keys assigned to each process.
It ends with all the keys stored on process 0 in sorted order. To achieve this, it
uses the same tree-structured communication that we used to implement a
global sum. However, when a process receives another process’ keys, it merges
the new keys into its already sorted list of keys. Write a program that
implements parallel mergesort. Process 0 should read in n and broadcast it to
the other processes. Each process should use a random number generator to
create a local list of n/comm_size ints. Each process should then sort its local
list, and process 0 should gather and print the local lists. Then the processes
should use tree-structured communication to merge the global list onto process
0, which prints the result. (MPI)

How to run?
mpic++ mergeSort.cpp -o mergeSort
mpirun -np 4 ./mergeSort

*/

#include <bits/stdc++.h>
#include <mpi.h>

using namespace std;

void merge(int arr[], int l, int m, int r)
{
    int n1 = m - l + 1;
    int n2 = r - m;
    int L[n1], R[n2];
 
    for (int i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (int j = 0; j < n2; j++)
        R[j] = arr[m + 1 + j];
 
    int i = 0, j = 0, k = l;
 
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k] = L[i];
            i++;
        }
        else {
            arr[k] = R[j];
            j++;
        }
        k++;
    }
 
    while (i < n1) {
        arr[k] = L[i];
        i++;
        k++;
    }
 
    while (j < n2) {
        arr[k] = R[j];
        j++;
        k++;
    }
}
 
void mergeSort(int arr[],int l,int r){
    if(l>=r){
        return;
    }
    int m =l+ (r-l)/2;
    mergeSort(arr,l,m);
    mergeSort(arr,m+1,r);
    merge(arr,l,m,r);
}

// Combines given 2 sorted arrays into one sorted array
int * mergeSpecial(int *a, int *b, int n) {

	int *c;
	c = (int*)malloc(2 * n * sizeof(int));

	int ai, bi, i;
	
	while( ai<n && bi<n)
	{
		if(a[ai]>b[bi]){
			c[i] = b[bi];
			bi++;
		}else{
			c[i] = a[ai];
			ai++;
		}
		i++;
	}

	while(ai<n && i<2*n ){
		c[i] = a[ai];
		i++;
		ai++;
	}

	while(bi<n && i<2*n ){
		c[i] = b[bi];
		i++;
		bi++;
	}

	free(a);
	free(b);
	return c;
}



int* fill_data(int n, int pid){
	int *data;
	data = (int*)malloc(n * sizeof(int));
	srand(pid+time(0));
	for (int i=0; i<n; i++) {
        data[i] = rand()%1000;
    }

    return data;
}

// Serial Merge sort
void serialMergeSort(int n){
	int *data = fill_data(n,0);
	mergeSort(data,0,n-1);
}

int main(int argc, char* argv[])
{

	double mytime, finalTime;
	int pid, np,
		elements_per_process;
	// np -> no. of processes
	// pid -> process id

	MPI_Status status;

	// Creation of parallel processes
	MPI_Init(&argc, &argv);

	// find out process ID,
	// and how many processes were started
	MPI_Comm_rank(MPI_COMM_WORLD, &pid);
	MPI_Comm_size(MPI_COMM_WORLD, &np);

	int n;
	if(pid==0){
		cout<<"Enter array size: ";
	    cin>>n;
    }

	MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
	elements_per_process = n / np;

	int *partial_sort = fill_data(elements_per_process, pid);
	int *receive_sorted;

	// Printing each process's individual data
	// by process 0
	if(pid == 0){

		cout<<"Process "<<pid<<" Data: \n";
		for (int i = 0; i < elements_per_process; ++i)
				cout<<partial_sort[i]<<" ";
		cout<<endl;

		for (int i = 1; i < np; ++i)
		{
			// Parent receving all the data
			int *local_process_data = (int*)malloc(elements_per_process * sizeof(int));
			MPI_Recv(local_process_data, elements_per_process, 
				MPI_INT, i, 1, MPI_COMM_WORLD, &status);
			
			cout<<"Process "<<i<<" Data: \n";
			for (int i = 0; i < elements_per_process; ++i)
				cout<<local_process_data[i]<<" ";
			cout<<endl;

			free(local_process_data);
		}
	}else{ 
		// Child process sends the data
		MPI_Send(partial_sort, elements_per_process, MPI_INT,
					0, 1, MPI_COMM_WORLD);	
	}

	// Start Time
	if(pid == 0)
		mytime = MPI_Wtime();

	// Classical merge sort for sorting local data
	// of every process
	mergeSort(partial_sort,0,elements_per_process-1);
	
	// Iterating for log2 np times to 
	// form tree like MPI Pattern
	for (int i = 0; i < log2(np); ++i)
	{
		// deviding the processes into half sections
		// half section will send data and other will receive
		int range = np/pow(2,i+1);

		// New size of the combined array at a perticualr iteration
		int newSize = elements_per_process*pow(2,i);
		
		if(pid<range){
			// First half 
			// Receives the data from second half processes

			receive_sorted = (int*)malloc(newSize * sizeof(int));
			//cout<<pid<<" < Waits "<<newSize<<" Receiving from > "<<pid+range<<endl;
			MPI_Recv(receive_sorted, newSize, MPI_INT, pid+range, 1, MPI_COMM_WORLD, &status);
		
		}else if(pid<range*2){
			// Second Half
			// Sends data to the First half processes

			//cout<< pid <<" < Waits "<<newSize<<" Sending to > "<<pid-range<<endl;
			MPI_Send(partial_sort, newSize, MPI_INT,
					pid-range, 1, MPI_COMM_WORLD);
			free(partial_sort);
			
			// Break out this process part is done.
			break;
		}

		// merge partial sort and received sorted and send again
		// now partial sort must contain 2*n sized array to send
		// after the mergeSpecial operation partial sort contains
		// the sorted data
		partial_sort = mergeSpecial(partial_sort, receive_sorted, newSize);
		
		if (pid==0)
			cout<<"Done Iteration :" << i+1<<endl;
	}


  	// Process 0 prints all the final data
	if(pid == 0){
		finalTime = MPI_Wtime();
		cout<<"Final data: \n";
		for (int i = 0; i < n; ++i)
			cout<<partial_sort[i]<<" ";
		cout << endl;
		free(partial_sort);

    	printf("Parallel Time: %lf\n", mytime - finalTime);
	}

	// Serial Implimentation
	if(pid==0){
		mytime = MPI_Wtime();
		serialMergeSort(n);
		finalTime = MPI_Wtime();
    	printf("Serial Time: %lf\n", mytime - finalTime);
	}
	
	// cleans up all MPI state before exit of process	
	MPI_Finalize();
	return 0;
}
