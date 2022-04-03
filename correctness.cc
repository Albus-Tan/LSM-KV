#include <iostream>
#include <cstdint>
#include <string>

#include "test.h"

class CorrectnessTest : public Test {
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX = 1024 * 64;

	void regular_test(uint64_t max)
	{
		uint64_t i;

		// Test a single key
		EXPECT(not_found, store.get(1));
		store.put(1, "SE");
		EXPECT("SE", store.get(1));
		EXPECT(true, store.del(1));
		EXPECT(not_found, store.get(1));
		EXPECT(false, store.del(1));

		phase();

		// Test multiple key-value pairs
		for (i = 0; i < max; ++i) {
			store.put(i, std::string(i+1, 's'));
			EXPECT(std::string(i+1, 's'), store.get(i));
		}
		phase();

		// Test after all insertions
		for (i = 0; i < max; ++i){
            EXPECT(std::string(i+1, 's'), store.get(i));
        }

		phase();

		// Test deletions
		for (i = 0; i < max; i+=2)
			EXPECT(true, store.del(i));

		for (i = 0; i < max; ++i)
			EXPECT((i & 1) ? std::string(i+1, 's') : not_found,
			       store.get(i));

		for (i = 1; i < max; ++i)
			EXPECT(i & 1, store.del(i));

		phase();

		// Test scan
		std::list<std::pair<uint64_t, std::string> > list_ans;
		std::list<std::pair<uint64_t, std::string> > list_stu;
		for (i = 0; i < max; ++i) {
			store.put(i, std::string(i+1, 's'));
			if (i < max / 2) list_ans.emplace_back(std::make_pair(i,std::string(i+1, 's')));
		}

		store.scan(0, max / 2 - 1, list_stu);
		EXPECT(list_ans.size(), list_stu.size());

		auto ap = list_ans.begin();
		auto sp = list_stu.begin();
		while(ap != list_ans.end()) {
			if (sp == list_stu.end()) {
				EXPECT((*ap).first, -1);
				EXPECT((*ap).second, not_found);
				ap++;
			}
			else {
				EXPECT((*ap).first, (*sp).first);
				EXPECT((*ap).second, (*sp).second);
				ap++;
				sp++;
			}
		}

		for (i = 0; i < max; i++)
			EXPECT(true, store.del(i));

		phase();

		report();
	}

public:
	CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Correctness Test" << std::endl;

		std::cout << "[Simple Test]" << std::endl;
		regular_test(SIMPLE_TEST_MAX);

		std::cout << "[Large Test]" << std::endl;
		regular_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	CorrectnessTest test("./data", verbose);

    try {
        test.start_test();
    } catch (const char * e){
        std::cout << e << std::endl;
    } catch (std::exception &e){
        std::cout << e.what() << std::endl; //捕获异常，然后程序结束
    }catch (...) {
        std::cout << "unknown error !" << std::endl;
    }


	return 0;
}