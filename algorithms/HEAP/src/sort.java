import java.util.Random;
public class sort {
	public static void main(String[] args) {
		Random randGen = new Random();
		int i;
		int[] array = new int[100 + 1];
		for(i = 1;i < array.length;i++)
			array[i] = randGen.nextInt(1000);
		Heap heap = new Heap(array,"max");
		heap.heapSort(array,"max");
		System.out.println("Sorted Array:");
		for(i = 1;i < heap.heapSize;i++)
			System.out.print(array[i] + " ");
		System.out.println();
	}
}
