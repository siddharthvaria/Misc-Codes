import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
class tnode{
int value;
tnode leftChild;
tnode rightChild;
tnode parent;
tnode next;
public tnode(int v){
	value = v;
	leftChild = null;
	rightChild = null;
	parent = null;
	next = null;
}
}
public class binarySearchTree {
	tnode root;
	public binarySearchTree(){
		root = null;
	}
	public void insert(int v){
		root = insert(root,v);
	}
	private tnode insert(tnode t,int v){
		if(t == null){
			t = new tnode(v);
			return t;
		}
		else{
			if(v <= t.value){
				t.leftChild = insert(t.leftChild,v);
				t.leftChild.parent = t;
			}
			else{
				t.rightChild = insert(t.rightChild,v);
				t.rightChild.parent = t;
			}
		}
		return t;
	}
	public void insertWithoutRecursion(int v){
		if(root == null)
			root = new tnode(v);
		else{
			tnode prev = null;
			tnode current = root;
			while(current != null){
				prev = current;
				if(v <= current.value)
					current = current.leftChild;
				else
					current = current.rightChild;
			}
			if(v <= prev.value){
				prev.leftChild = new tnode(v);
				prev.leftChild.parent = prev;
			}
			else{
				prev.rightChild = new tnode(v);
				prev.rightChild.parent = prev;
			}
		}
	}
	public void inorder(){
		if(root == null)
			return;
		System.out.println("Inorder Traversal:");
		long startTime = System.currentTimeMillis();
		inorder(root);
		long endTime = System.currentTimeMillis();
		System.out.println("inorder:" + (endTime - startTime) + " Millis");
	}
	private void inorder(tnode t){
		if(t == null)
			return;
		else{
			inorder(t.leftChild);
			System.out.print(t.value + " ");
			inorder(t.rightChild);
		}
	}
	public void inorderUsingStack(){
		if(root == null)
			return;
		inorderUsingStack(root);
	}
	private void inorderUsingStack(tnode t){
		Stack<tnode> stack = new Stack<tnode>();
		long startTime = System.currentTimeMillis();
		while(!stack.isEmpty() || t != null){
			if(t != null){
				stack.push(t);
				t = t.leftChild;
			}
			else{
				t = stack.pop();
				System.out.print(t.value + " ");
				t = t.rightChild;
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("inorderUsingStack:" + (endTime - startTime) + " Millis");
	}
	public void inorderWithoutUsingStack(){
		if(root == null)
			return;
		inorderWithoutUsingStack(root);
	}
	private void inorderWithoutUsingStack(tnode t){
		/*
		 * Algorithm taken from http://www.geeksforgeeks.org/archives/6358
		 	1. Initialize current as root 
			2. While current is not NULL
   			If current does not have left child
      			a) Print current’s data
      			b) Go to the right, i.e., current = current->right
   			Else
      			a) Make current as right child of the rightmost node in current's left subtree
      			b) Go to this left child, i.e., current = current->left
		 */
		tnode current = t;
		tnode pre;
		long startTime = System.currentTimeMillis();
		while(current != null){
			if(current.leftChild == null){
				System.out.print(current.value + " ");
				current = current.rightChild;
			}
			else{
				pre = current.leftChild;
				while(pre.rightChild != null && pre.rightChild != current)
					pre = pre.rightChild;
				if(pre.rightChild == null){
					pre.rightChild = current;
					current = current.leftChild;
				}
				else{
					pre.rightChild = null;
					System.out.print(current.value + " ");
					current = current.rightChild;
				}
			}	
		}
		long endTime = System.currentTimeMillis();
		System.out.println("inorderWithoutUsingStack:" + (endTime - startTime) + " Millis");
	}
	public void preorder(){
		if(root == null)
			return;
		System.out.println("Preorder Traversal:");
		long startTime = System.currentTimeMillis();
		preorder(root);
		long endTime = System.currentTimeMillis();
		System.out.println("preorder:" + (endTime - startTime) + " Millis");
	}
	private void preorder(tnode t){
		if(t == null)
			return;
		else{
			System.out.print(t.value + " ");
			preorder(t.leftChild);
			preorder(t.rightChild);
		}
	}
	public void preorderUsingStack(){
		if(root == null)
			return;
		preorderUsingStack(root);
	}
	private void preorderUsingStack(tnode t){
		Stack<tnode> stack = new Stack<tnode>();
		long startTime = System.currentTimeMillis();
		while(!stack.isEmpty() || t != null){
			if(t != null){
				System.out.print(t.value + " ");
				stack.push(t);
				t = t.leftChild;
			}
			else{
				t = stack.pop();
				t = t.rightChild;
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("preorderUsingStack:" + (endTime - startTime) + " Millis");
	}
	public void preorderWithoutUsingStack(){
		if(root == null)
			return;
		preorderWithoutUsingStack(root);
	}
	private void preorderWithoutUsingStack(tnode t){
		tnode current = t;
		tnode pre;
		long startTime = System.currentTimeMillis();
		while(current != null){
			if(current.leftChild == null){
				System.out.print(current.value + " ");
				current = current.rightChild;
			}
			else{
				pre = current.leftChild;
				while(pre.rightChild != null && pre.rightChild != current)
					pre = pre.rightChild;
				if(pre.rightChild == null){
					System.out.print(current.value + " ");
					pre.rightChild = current;
					current = current.leftChild;
				}
				else{
					pre.rightChild = null;
					current = current.rightChild;
				}
			}	
		}
		long endTime = System.currentTimeMillis();
		System.out.println("preorderWithoutUsingStack:" + (endTime - startTime) + " Millis");
}
	public void postorder(){
		
	}
	public void postorderUsingStack(){
		if(root == null)
			return;
		postorderUsingStack(root);		
	}
	private void postorderUsingStack(tnode t){
		Stack<tnode> stack = new Stack<tnode>();
		long startTime = System.currentTimeMillis();
		stack.push(t);
		tnode prevNode = null;
		while(!stack.isEmpty()){
			t = stack.peek();
			if(prevNode == null || prevNode.leftChild == t || prevNode.rightChild == t){
				if(t.leftChild != null){
					stack.push(t.leftChild);
				}
				else if(t.rightChild != null){
					stack.push(t.rightChild);
				}
			}
			else if(t.leftChild == prevNode){
				if(t.rightChild != null){
					stack.push(t.rightChild);
				}
			}
			else{
				System.out.println(t);
				stack.pop();
			}
			prevNode = t;
		}
		long endTime = System.currentTimeMillis();
		System.out.println("postorderUsingStack:" + (endTime - startTime) + " Millis");
	}
	public void postorderWithoutUsingStack(){
		
	}
	public void Delete(int v){
		if(root == null){
			System.out.println("v not found");
			return;
		}
		if(root.value == v){
			if(root.leftChild == null){
				root = root.rightChild;
			}
			else if(root.rightChild == null)
				root = root.leftChild;
		}
		else if(v < root.value)
			Delete(v,root,root.leftChild);
		else
			Delete(v,root,root.rightChild);
	}
	private void Delete(int v,tnode parent,tnode n){
		if(n == null)
			System.out.println("v not found");
		else if(v == n.value){
			if(n.leftChild == null){
				if(n == parent.leftChild)
					parent.leftChild = n.rightChild;
				else
					parent.rightChild = n.rightChild;
			}
			else if(n.rightChild == null){
				if(n == parent.leftChild)
					parent.leftChild = n.leftChild;
				else
					parent.rightChild = n.leftChild;
			}
			else{
				tnode tmp1 = n.rightChild;
				tnode tmp2 = tmp1.leftChild;
				if(tmp2 == null){
					n.value = tmp1.value;
					n.rightChild = tmp1.rightChild;
				}
				else{
					while(tmp2.leftChild != null){
						tmp1 = tmp2;
						tmp2 = tmp2.leftChild;
					}
					n.value = tmp2.value;
					tmp1.leftChild = null;
				}
			}
		}
		else if(v < n.value){
			Delete(v,n,n.leftChild);
		}
		else{
			Delete(v,n,n.rightChild);
		}
	}
	public void addSideLinks(){
		System.out.println(this.root.value);
		this.root.next = null;
		Queue<tnode> q = new LinkedList<tnode>();
		if(this.root.leftChild != null)
			q.add(root.leftChild);
		if(this.root.rightChild != null)
			q.add(root.rightChild);
		Queue<tnode> qdash = new LinkedList<tnode>();
		while(!q.isEmpty()){
			tnode tmp = q.poll();
			System.out.print(tmp.value + " ");
			if(!q.isEmpty())
				tmp.next = q.peek();
			else
				tmp.next = null;
			if(tmp.leftChild != null)
				qdash.add(tmp.leftChild);
			if(tmp.rightChild != null)
				qdash.add(tmp.rightChild);
			if(q.isEmpty()){
				q = qdash;
				qdash = new LinkedList<tnode>();
				System.out.println();
			}
		}
	}
	public static void main(String[] args){
		int i;
		binarySearchTree bst = new binarySearchTree();
		for(i = 0;i < 10;i++){
			bst.insert((int)(Math.random()*1000));
		}
		bst.inorder();
		bst.preorder();
		bst.addSideLinks();
	}
}
