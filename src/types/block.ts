export interface Block {
  id: string;
  content: string;
  parentId: string | null;
  children: Block[];
  collapsed?: boolean;
  createdAt: number;
  updatedAt: number;
}

export interface BlockNode {
  id: string;
  name: string;
  children?: BlockNode[];
  data: Block;
}
