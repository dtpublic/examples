## Sample application to illustrate use of unifiers.

There are 3 operators [class names in brackets]:
+ **random**  [RandomInteger]
+ **range**   [RangeFinder]
+ **console** [ToConsole]

The first generates random integers and emits them; the second computes the range of
integers received in each window as a pair of values (high, low) and emits it; the
third writes the range to the console along with the corresponding window id and
timestamp.

The default configuration (`META-INF/properties.xml`) partitions the **range** operator
into 3 replicas.

When run on a cluster with the default configuration, the default pass-through unifier
is used and the standard output of the console operator shows up to 3 lines per window
like this (which is not the desired output since we expect only one range per window):
```
tuple = (-2142821900,2130498796), window = 6294004942831091779, time = 1465437255081 (s)
tuple = (-2108297670,2143701598), window = 6294004942831091779, time = 1465437255082 (s)
tuple = (-2128869319,2146342673), window = 6294004942831091779, time = 1465437255082 (s)
```
When run on a cluster with the `use-unifier.xml` configuration, a range unifier from the
_Malhar_ library is used and the standard output of the console operator shows one line
per window like this (which is the desired output and matches what we see when no
partitioning is involved):
```
tuple = (-2146801472,2145945795), window = 6293990108014052044, time = 1465434125937 (s)
```
The same difference in behavior with and without a unifier is illustrated in the pair
of tests in `ApplicationTests.java`: `testApplicationWithoutUnifier()` and
`testApplicationWithUnifier()`.
