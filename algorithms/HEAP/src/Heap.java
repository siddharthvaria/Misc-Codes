
public class Heap {
	protected int heapSize;
	public Heap(int[] arr,String type){
		buildHeap(arr,type);
	}
	protected void heapify(int[] arr,String type,int index){

		//heapify(arr,"max",index)		for max-heaps
		//heapify(arr,"min",index)		for min-heaps

		int largest_or_smallest;
		int l = 2 * index;
		int r = 2 * index + 1;
		do{
			if(type.equals("max")){
			if(l <= heapSize && arr[index] < arr[l])
				largest_or_smallest = l;
			else
				largest_or_smallest = index;
			if(r <= heapSize && arr[largest_or_smallest] < arr[r] )
				largest_or_smallest = r;
			}
			else{
				if(l <= heapSize && arr[index] > arr[l])
					largest_or_smallest = l;
				else
					largest_or_smallest = index;
				if(r <= heapSize && arr[largest_or_smallest] > arr[r] )
					largest_or_smallest = r;
			}
			if(largest_or_smallest != index){
				int tmp = arr[largest_or_smallest];
				arr[largest_or_smallest] = arr[index];
				arr[index] = tmp;
				l = 2 * largest_or_smallest;
				r = 2 * largest_or_smallest + 1;
				index = largest_or_smallest;
				}
			else
				break;
			}while(l <= heapSize && r <= heapSize);
		}
	protected void buildHeap(int[] arr,String type){
		int i;
		heapSize = arr.length - 1;
		for(i = (arr.length - 1) / 2;i > 0;i--)
			heapify(arr,type,i);
	}
	protected void heapSort(int[] arr,String type){
		int i;
		int tmp;
		for(i = arr.length - 1;i > 1;i--){
			tmp = arr[i];
			arr[i] = arr[1];
			arr[1] = tmp;
			heapSize--;
			heapify(arr,type,1);
		}
	}
	
	
	protected int returnMaximum(int[] arr,String type){
		int max;
		int i;
		if(type.equals("max"))
		return arr[1];
		else{
			max = Integer.MIN_VALUE;
			for(i = (arr.length - 1) / 2;i < arr.length;i++){
				if(arr[i] > max){
					max = arr[i];
				}
			}
			return max;
		}
	}
	protected int returnMinimum(int[] arr,String type){
		int min;
		int i;
		if(type.equals("max"))
			return arr[1];
		else{
			min = Integer.MIN_VALUE;
			for(i = (arr.length - 1) / 2;i < arr.length;i++){
				if(arr[i] < min){
					min = arr[i];
				}
			}
			return min;
		}
	}  
	protected int extractMax(int arr[],String type){
		int max;
		int i;
		int index = 0;
		int tmp;
		if(type.equals("max")){
			max = arr[1];
			arr[1] = arr[heapSize];
			heapSize--;
			heapify(arr,type,1);
		}
		else {
			max = Integer.MIN_VALUE;
			// max value is one of the leaf nodes
			for(i = (arr.length - 1) / 2;i < arr.length;i++){
				if(arr[i] > max){
					max = arr[i];
					index = i;
				}
			}
			arr[index] = arr[heapSize];
			i = index;
			while(i > 1 && arr[i / 2] > arr[i]){
			tmp = arr[i / 2];
			arr[i / 2] = arr[i];
			arr[i] = tmp;
			i = i / 2;
			}
			heapSize--;
		}
		return max;
	}
	protected int extractMin(int[] arr,String type){
		int min;
		int i;
		int index = 0;
		int tmp;
		if(type.equals("min")){
			min = arr[1];
			arr[1] = arr[heapSize];
			heapSize--;
			heapify(arr,type,1);
		}
		else{
			min = Integer.MAX_VALUE;
			for(i = (arr.length - 1) / 2;i < arr.length;i++){
				if(arr[i] < min){
					min = arr[i];
					index = i;
				}
			}
			arr[index] = arr[heapSize];
			i = index;
			while(i > 1 && arr[i / 2] < arr[i]){
			tmp = arr[i / 2];
			arr[i / 2] = arr[i];
			arr[i] = tmp;
			i = i / 2;
			}
			heapSize--;
		}
		return min;
	}

	
}
