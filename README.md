# StripesAlgorithm
Hadoop MapReduce Job
Crystal ball to predict events that may happen once a certain event happened - Stripe Approach

Mapper Pseudo code

class Mapper
	
   method Map(docid a; doc d)

	for all term w in doc d do

		H = new AssociativeArray()

		for all term u in Neighbors(w) do
				H{u} = H{u} + 1

		Emit(term w; stripe H)
		
	Reducer Pseudo code

class Reducer
	
 method Reduce(term w; stripes[H1;H2;H3; …])
	Hf = new AssociativeArray()
	marginal = 0
	
	for all stripe H in stripes [H1;H2;H3; …] do
		for all term u in H do
			Hf{u} = Hf{u} + H{u}
			 maginal = marginal + H{u}
 
	for all term u in Hf do
		Hf{u} = Hf{u} / marginal		
	
	Emit(term w; stripe Hf) 
