Author: David Smiley

You may find supplemental instructions online:
http://wiki.apache.org/solr/SpatialSearch#SOLR-2155

Instructions:

First, you will need to build the jar file from source if the jar wasn't provided.
This is as simple as running "mvn install" which generates a jar file: target/Solr2155-1.0.3.jar
Put that on Solr's classpath similar to how other Solr contrib jars are installed.

Now edit some config files.

   schema.xml:
        <fieldType name="geohash" class="solr2155.solr.schema.GeoHashField" length="12" />

        <field name="store" type="geohash" indexed="true" stored="true" multiValued="true"/>

   solrconfig.xml:
      Top level within <config>, suggested to place at bottom:
        <!-- an alternative query parser to geofilt() (notably allows a specific lat-lon box) -->
        <queryParser name="gh_geofilt" class="solr2155.solr.search.SpatialGeoHashFilterQParser$Plugin" />
        <!-- replace built-in geodist() with our own modified one -->
        <valueSourceParser name="geodist" class="solr2155.solr.search.function.distance.HaversineConstFunction$HaversineValueSourceParser" />
        
      Add the following cache into the <query> section if you are going to use the geodist function:
          <!-- SOLR-2155 -->
          <cache name="fieldValueCache"
            class="solr.FastLRUCache"
            size="10"
            initialSize="1"
            autowarmCount="1"/>
      Note that the name of this cache is unfortunate; it should have been something like "solr2155", and instead
      it is confusingly the same name as another cache although it is distinct from it.

At this point you can use Solr's {!geofilt}, {!bbox}, and {!geodist} as documented. You can also use
{!gh_geofilt} like so:  (args are in west,south,east,north order):
  fq={!gh_geofilt sfield=store box="-98,35,-97,36"}
For further info on gh_geofilt, see the well-documented source.

CHANGES

 1.0.5: * Fixed bug affecting sorting by distance when the index was not in an optimized state.
        * Norms are omitted automatically now; they aren't used.

 1.0.4: Fixed bug in which geohashes were returned from Solr xml responses instead of lat,lon. Enhanced README.txt
        and Solr wiki.

 1.0.3: * exception text for the absent sfield local param;
        * add cache enabling recommendation into README.txt (cache name is confusing a little)
        * fix for UnsupportedOpEx on debugQuery=on for geodist func (but my toString() impl seems overcomplicated)

 1.0.2: (first public release)