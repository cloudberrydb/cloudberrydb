#include "delete_bitmap_c.h"
#include "delete_bitmap.h"

void *
createBitmap(void)
{
	std::string errorMessage;
	roaring::Roaring64Map *bitmap = NULL;

	try
	{
		bitmap = cppCreateBitmap();
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to create bitmap: %s", errorMessage.c_str());

	return bitmap;
}

void
destroyBitmap(void *bitmap)
{
	std::string errorMessage;
	roaring::Roaring64Map *roaring = (roaring::Roaring64Map *) bitmap;

	try
	{
		cppDestroyBitmap(roaring);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to destroy: %s", errorMessage.c_str());
}

void
bitmapAdd(void *bitmap, uint64_t value)
{
	std::string errorMessage;
	roaring::Roaring64Map *roaring = (roaring::Roaring64Map *) bitmap;

	try
	{
		cppBitmapAdd(roaring, value);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to add value to bitmap: %s", errorMessage.c_str());
}

bool
bitmapContains(void *bitmap, uint64_t value)
{
	bool result = false;
	std::string errorMessage;
	roaring::Roaring64Map *roaring = (roaring::Roaring64Map *) bitmap;

	try
	{
		result = cppBitmapContains(roaring, value);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to check value is contained by bitmap: %s", errorMessage.c_str());
	
	return result;
}
