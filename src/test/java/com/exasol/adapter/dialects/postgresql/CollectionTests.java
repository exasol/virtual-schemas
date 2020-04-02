package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class CollectionTests {
    @Test
    void testContainsAll() {
        assertThat("[A]-->[A]", List.of("A").containsAll(List.of("A")), equalTo(true));
        assertThat("[A, B]-->[A]", List.of("A", "B").containsAll(List.of("A")), equalTo(true));
        assertThat("[A]-->[A,B]", List.of("A").containsAll(List.of("A", "B")), equalTo(false));
    }
}
