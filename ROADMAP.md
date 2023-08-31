## Pandasaurus_cxg Roadmap

* Generate & release integrated doc from PyDoc - including links to Tutorial notebooks

    (potential framework - Sphinx)

* Testing:
   * Test against a range of datasets on CxG to find bugs and performance issues
   * User testing - recruit friendly bioinformaticians to give feedback on functionality and usability

* Extend basic enrichment methods to include number of hops from term.

* Add support for CxG schema validation (via dependency on official lib)

  This may not be needed for files downloaded from CxG, but aim is in part to promote the standard more generally so aims to be ready for files from other sources.

* Add semantic context queries
  
  (Dependency - add abstracted most-specific subject/object queries to pandasaurus)
  * CL-Pro
  * CL-GO & GO-CL
  * HPO-CL & MP-CL
  * MONDO-CL
  * OBA-CL
 
* Add interface to QuickGO to pull gene associations.
  
  Can we use an existing lib for this?

* Add interface to Monarch API to pull gene associations for Mondo, HP, MP, OBA.
  
  Can we use an existing lib for this or collaborate with Monarch on one?

* Add support for queries for gene sets and general classes from disease metadata term.

* Extend support for filtering on metadata before analysis
  
* Add library of author cell type fields for CxG hosted datasets where this has been curated

* Add support for cell type annotation schema (CAP)


## Potential future functionality

Both of these are probably better served by workflows with existing libraries

- Automatic Cross checking retrieved gene sets against cluster expression
- interfacing with standard enrichment tools 




