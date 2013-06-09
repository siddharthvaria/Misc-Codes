import java.util.Scanner;
import java.io.*;
public class testMergeSort {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		int arraySize;
		String fileName;
		Scanner s = null;
		fileName = args[0];
		File f = new File(fileName);
		try {
			s = new Scanner(f);
		} catch (FileNotFoundException e) {
			System.out.println("File not found");
			e.printStackTrace();
		}
		arraySize = s.nextInt();
		int[] array = new int[arraySize];
		int count = 0;
		while(s.hasNext()){
			array[count++] = s.nextInt();
		}
		s.close();
		System.out.println("Length of file in terms of number of elements:" + array.length);
		mergeSort.merge_sort(array, 1, array.length);
		fileName = "sorted" + fileName;
		f = new File(fileName);
		PrintWriter output = new PrintWriter(f);
		output.println("" + array.length);
		for(count = 1; count <= array.length; count++){
			output.print(array[count-1] + " ");
			if(count % 10 == 0) output.println();
		}	
	}
}
