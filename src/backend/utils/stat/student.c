/* -----------------------------------------------------------------------------
 *
 * student.c
 *
 * Evaluate the Student-T distribution function.
 *
 * Copyright (c) 2010, EMC
 * 
 * -----------------------------------------------------------------------------
 *
 * Author: Florian Schoppmann
 * Date:   November 2010
 * 
 * This file is compliant with the C99 standard (Compile with "-std=c99").
 *
 * Emprirical results indicate that the numerical quality of the series
 * expansion from [1] (see notes below) is vastly superior to using continued
 * fractions for computing the cdf via the incomplete beta function.
 *
 * Main reference:
 *
 * [1] Abramowitz and Stegun, Handbook of Mathematical Functions with Formulas,
 *     Graphs, and Mathematical Tables, 1972
 *     page 948: http://people.math.sfu.ca/~cbm/aands/page_948.htm
 * 
 * Further reading (for computing the Student-T cdf via the incomplete beta
 * function):
 *
 * [2] NIST Digital Library of Mathematical Functions, Ch. 8,
 *     Incomplete Gamma and Related Functions,
 *     http://dlmf.nist.gov/8.17
 * [3] Lentz, Generating Bessel functions in Mie scattering calculations using
 *     continued fractions, Applied Optics, Vol. 15, No. 3, 1976
 * [4] Thompson and Barnett, Coulomb and Bessel Functions of Complex Arguments
 *     and Order, Journal of Computational Physics, Vol. 64, 1986
 * [5] Cuyt et al., Handbook of Continued Fractions for Special Functions,
 *     Springer, 2008
 * [6] Gil et al., Numerical Methods for Special Functions, SIAM, 2008
 * [7] Press et al., Numerical Recipes in C++, 3rd edition,
 *     Cambridge Univ. Press, 2007
 * [8] DiDonato, Morris, Jr., Algorithm 708: Significant Digit Computation of
 *     the Incomplete Beta Function Ratios, ACM Transactions on Mathematical
 *     Software, Vol. 18, No. 3, 1992
 *
 * Approximating the Student-T distribution function with the normal
 * distribution:
 *
 * [9]  Gleason, A note on a proposed student t approximation, Computational
 *      Statistics & Data Analysis, Vol. 34, No. 1, 2000
 * [10] Gaver and Kafadar, A Retrievable Recipe for Inverse t, The American
 *      Statistician, Vol. 38, No. 4, 1984
 */

#include "postgres.h"
 
#include "student.h"

#include <math.h>


/* Prototypes of internal functions */

static inline float8 normal_cdf(float8 t);
static inline float8 studentT_cdf_approx(uint64 nu, float8 t);


/* 
 * float8 studentT_cdf(int64 nu, float8 t)
 * -----------------------------------------------
 *
 * Compute Pr[T <= t] for Student-t distributed T with nu degrees of freedom.
 *
 * For nu >= 1000000, we just use the normal distribution as an approximation.
 * For 1000000 >= nu >= 200, we use a simple approximation from [9].
 * We are much more cautious than usual here (it is folklore that the normal
 * distribution is a "good" estimate for Student-T if nu >= 30), but we can
 * afford the extra work as this function is not designed to be called from
 * inner loops. Performance should still be reasonably good, with at most ~100
 * iterations in any case (just one if nu >= 200).
 * 
 * For nu < 200, we use the series expansions 26.7.3 and 26.7.4 from [1] and
 * substitute sin(theta) = t/sqrt(n * z), where z = 1 + t^2/nu.
 *
 * This gives:
 *                          t
 *   A(t|1)  = 2 arctan( -------- ) ,
 *                       sqrt(nu)
 *
 *                                                    (nu-3)/2
 *             2   [            t              t         --    2 * 4 * ... * (2i)  ]
 *   A(t|nu) = - * [ arctan( -------- ) + ------------ * \  ---------------------- ]
 *             π   [         sqrt(nu)     sqrt(nu) * z   /_ 3 * ... * (2i+1) * z^i ]
 *                                                       i=0
 *           for odd nu > 1, and
 *
 *                         (nu-2)/2
 *                  t         -- 1 * 3 * ... * (2i - 1)
 *   A(t|nu) = ------------ * \  ------------------------ for even nu,
 *             sqrt(nu * z)   /_ 2 * 4 * ... * (2i) * z^i
 *                            i=0
 *
 * where A(t|nu) = Pr[|T| <= t].
 *
 * Note: The running time of calculating the series is proportional to nu. This
 * We therefore use the normal distribution as an approximation for large nu.
 * Another idea for handling this case can be found in reference [8].
 */

float8 
studentT_cdf(uint64 nu, float8 t) 
{
	float8		z,
				t_by_sqrt_nu;
	float8		A, /* contains A(t|nu) */
				prod = 1.,
				sum = 1.;

	/* Handle extreme cases. See above. */
	if (nu == 0)
		return NAN;
	else if (isinf(t))
		return signbit(t) ? 0 : 1;
	else if (nu >= 1000000)
		return normal_cdf(t);
	else if (nu >= 200)
		return studentT_cdf_approx(nu, t);

	/* Handle main case (nu < 200) in the rest of the function. */

	z = 1. + t * t / nu;
	t_by_sqrt_nu = fabs(t) / sqrt(nu);
	
	if (nu == 1)
	{
		A = 2. / M_PI * atan(t_by_sqrt_nu);
	}
	else if (nu & 1) /* odd nu > 1 */
	{
		for (int j = 2; j <= nu - 3; j += 2)
		{
			prod = prod * j / ((j + 1) * z);
			sum = sum + prod;
		}
		A = 2 / M_PI * ( atan(t_by_sqrt_nu) + t_by_sqrt_nu / z * sum );
	}
	else /* even nu */
	{
		for (int j = 2; j <= nu - 2; j += 2)
		{
			prod = prod * (j - 1) / (j * z);
			sum = sum + prod;
		}
		A = t_by_sqrt_nu / sqrt(z) * sum;
	}
	
	/* A should obviously lie withing the interval [0,1] plus minus (hopefully
	 * small) rounding errors. */
	if (A > 1.)
		A = 1.;
	else if (A < 0.)
		A = 0.;
	
	/* The Student-T distribution is obviously symmetric around t=0... */
	if (t < 0)
		return .5 * (1. - A);
	else
		return 1. - .5 * (1. - A);
}

/*
 * float8 normal_cdf(float8 t)
 * ---------------------------
 *
 * Compute the normal distribution function using the library error function.
 * This approximation satisfies
 * rel_error < 0.0001 || abs_error < 0.00000001
 * for all nu >= 1000000. (Tested on Mac OS X 10.6, gcc-4.2.)
 */

static inline float8 normal_cdf(float8 t)
{
	return .5 + .5 * erf(t / sqrt(2.));
}


/* float8 studentT_cdf_approx(uint64 nu, float8 t)
 * ------------------------------------------------------
 * 
 * We approximate the the Studen-T distribution using a formula suggested in
 * [9], which goes back to an approximation suggested in [10].
 *
 * Compared to the series expansion, this approximation satisfies
 * rel_error < 0.0001 || abs_error < 0.00000001
 * for all nu >= 200. (Tested on Mac OS X 10.6, gcc-4.2.)
 */

static inline float8 studentT_cdf_approx(uint64 nu, float8 t)
{
	float8	g = (nu - 1.5) / ((nu - 1) * (nu - 1)),
			z = sqrt( log(1. + t * t / nu) / g );

	if (t < 0)
		z *= -1.;
	
	return normal_cdf(z);
}
