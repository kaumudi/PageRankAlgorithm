# Template for writing MapReduce programs using mrjob
# % python mrjob-pagerank.py <input file>  -q

from mrjob.job import MRJob
from mrjob.step import MRStep

class MR_program(MRJob):

    def configure_args(self):
        '''
        This function takes the command line arguments and reads them into variables
        :return: none
        '''
        super(MR_program, self).configure_args()
        self.add_passthru_arg('--nodes')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('--beta')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('-N')    ##  <--- specify which new command line args are used

    def mapper_1(self, _, line):
        '''

        :param _: line from file
        :param line:
        :return: yields 2 things. One- each neighbour, page rank; Two - node,all neighbours
        '''
        num_nodes = int(self.options.nodes)
        #calculate initial page rank for each node
        init_pagerank = 1/num_nodes
        nodes = line.split()
        #putting all neighbours into a different list
        neighbour = nodes[1:]
        for l in neighbour:
            #yielding each neighbour and page rank
            yield l, init_pagerank/len(neighbour)
        #yielding each node of the graph and all neighbours
        yield line[0],neighbour

    def reducer_1(self, x, pr_y_by_n_or_nbrs_of_x):
        '''

        :param x: This is the node from the graph
        :param pr_y_by_n_or_nbrs_of_x: This is each node's list of page ranks
        :return: yields all the neighbours and corresponding page ranks, and also yields all the node and corresponding neighbours for each node
        '''
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        lst = list(pr_y_by_n_or_nbrs_of_x)
        zs = [] #all the neighbours into a list
        prs = [] #all page ranks for each node
        for x1 in lst:
            if isinstance(x1,list):
                zs.append(x1)
            if isinstance(x1,float):
                prs.append(x1)
        sum = 0 #sum of pr(y)/out(y)
        #making the z's list flat
        zs_flat_list = [item for sublist in zs for item in sublist]
        if prs:
            for x2 in prs:
                sum +=x2 #calculating sum of all pr's
        #updated page rank
        prx = (1-beta)/num_nodes + beta * (sum)
        for z in zs_flat_list:
            #printing the trace output
            print(z, prx/len(zs_flat_list))
            #yielding neighbours, and page ranks for each
            yield z,prx/len(zs_flat_list)
        #yielding each node and its neighbours
        yield x, zs_flat_list

    def reducer_2(self, x, pr_y_by_n_or_nbrs_of_x):
        '''

        :param x: This is the node from the graph
        :param pr_y_by_n_or_nbrs_of_x: This is each node's list of page ranks
        :return: yields each node and corresponding page rank as output
        '''
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        lst = list(pr_y_by_n_or_nbrs_of_x)
        #doing similar as above reducer 2
        zs = []
        prs = []
        for x1 in lst:
            if isinstance(x1, list):
                zs.append(x1)
            if isinstance(x1, float):
                prs.append(x1)
        sum = 0
        zs_flat_list = [item for sublist in zs for item in sublist]
        if prs:
            for x2 in prs:
                sum += x2
        prx = (1 - beta) / num_nodes + beta * (sum)
        #yielding each node's page rank
        yield x, prx


    def steps(self):
        N = int(self.options.N)
        return [MRStep(mapper=self.mapper_1)] + \
               [MRStep(reducer=self.reducer_1)]*N + \
               [MRStep(reducer=self.reducer_2)]
               

if __name__ == '__main__':
    # change to match the name of the class
    MR_program.run()
    
