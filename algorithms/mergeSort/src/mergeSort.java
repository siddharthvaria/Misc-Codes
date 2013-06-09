public class mergeSort {
	public static void merge_sort(int[] array,int s,int e){
	    if(s<e){
	    int m=(s+e)/2;
	    merge_sort(array,s,m);
	    merge_sort(array,m+1,e);
	    merge(array,s,m,e);
	}
	}
    public static void merge(int[] array,int s,int m,int e){
        int ln=m-s+1;
        int rn=e-m;
        int[] larr=new int[ln+1];
        int[] rarr=new int[rn+1];
        for(int count=0;count<ln;count++)
        larr[count]=array[s+count-1];
        for(int count=0;count<rn;count++)
        rarr[count]=array[m+count];
        int lc=0;
        int rc=0;
        boolean lo=false;
        boolean ro=false;
        int count;
        for(count=s-1;count<e;count++){
        if(lc>m-s){
        lo=true;
        break;
    }
        if(rc>e-m-1){
        ro=true;
        break;
    }
        if (larr[lc]>rarr[rc]){
        array[count]=rarr[rc];
        rc++;
    }
        else{
        array[count]=larr[lc];
        lc++;
    }
    }
        if(lo==true){
        for(int count1=rc;count1<e-m;count1++,count++)
        array[count]=rarr[count1];
    }
        if(ro==true){
        for(int count1=lc;count1<(m-s+1);count1++,count++)
        array[count]=larr[count1];
    }
    }
}
