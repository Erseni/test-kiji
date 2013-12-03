package net.mashlah.test.kiji;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PersonDaoTest {

    private final static Logger logger = LoggerFactory.getLogger(PersonDaoTest.class);

    private PersonDao dao;

    final Random random = new Random();


    @Before
    public void setUp() throws IOException {
        dao = new PersonDao(2181, new String[] {"localhost"}, "default");
        dao.deletePersons();
    }

    @Test
    public void testStorePerson() throws Exception {
        Person person = new Person();
        person.setId(10);
        person.setName("Erik");
        byte[] image = new byte[1024];
        random.nextBytes(image);
        for (int i = 0; i < 1000; i++) {
            dao.storePerson(person, image);
        }
    }

    @Test
    public void testMultiThreadingStore() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<Future> futures = new ArrayList();

        for (int i = 0; i < 1000; i++) {
            final int id = i;
            Future<?> f = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Person person = new Person();
                    person.setId(id);
                    person.setName("Erik" + id);
                    try {
                        byte[] image = new byte[1024];
                        random.nextBytes(image);
                        dao.storePerson(person, image);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(f);
        }
        boolean failed = false;
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.error("Error", e);
                failed = true;
            }
        }
        if (failed) {
            logger.error("One or more task failed");
        }

    }



}
